#!/usr/bin/env bash
# Launch a SPRUCE job on EMR Serverless and follow its progress.
#
# All settings can be supplied via env vars (handy for re-runs):
#   APPLICATION_ID, JOB_ROLE_ARN, JAR_S3, INPUT_S3, OUTPUT_S3, JOB_NAME,
#   PROVIDER (AWS, GOOGLE, AZURE), FORMAT (NATIVE, FOCUS),
#   EXEC_CORES, EXEC_MEMORY, EXEC_INSTANCES, DRIVER_CORES, DRIVER_MEMORY,
#   POLL_INTERVAL, FOLLOW (yes/no), EMR_JAVA_HOME,
#   LOG_S3 (optional S3 prefix for driver/executor logs, in addition to managed storage)
#
# Anything not set falls back to a default and is offered as an interactive prompt.
# APPLICATION_ID and JOB_ROLE_ARN are discovered via the AWS CLI when not set.
#
# After a successful run, the answers are saved to .launchEMR in the current
# directory (override with LAUNCH_EMR_STATE) and offered as the defaults next
# time; press Enter to keep a saved value or type a new one.

set -euo pipefail

# --- defaults ---------------------------------------------------------------
DEFAULT_JOB_NAME="SPRUCE"
DEFAULT_PROVIDER="AWS"
DEFAULT_FORMAT="NATIVE"
DEFAULT_EXEC_CORES="4"
DEFAULT_EXEC_MEMORY="4g"
DEFAULT_EXEC_INSTANCES="2"
DEFAULT_DRIVER_CORES="1"
DEFAULT_DRIVER_MEMORY="4g"
DEFAULT_POLL_INTERVAL="30"
DEFAULT_FOLLOW="yes"

# --- saved answers from the last successful run -----------------------------
STATE_FILE="${LAUNCH_EMR_STATE:-.launchEMR}"
SAVED_KEYS=(APPLICATION_ID JOB_ROLE_ARN JAR_S3 INPUT_S3 OUTPUT_S3 PROVIDER FORMAT JOB_NAME
            EXEC_CORES EXEC_MEMORY EXEC_INSTANCES DRIVER_CORES DRIVER_MEMORY LOG_S3)

load_saved() {
    # Plain KEY=VALUE lines, read rather than sourced so the file can't run code.
    [[ -f "$STATE_FILE" ]] || return 0
    local key value k
    while IFS='=' read -r key value; do
        for k in "${SAVED_KEYS[@]}"; do
            [[ "$key" == "$k" ]] && printf -v "SAVED_${k}" '%s' "$value"
        done
    done < "$STATE_FILE"
    echo "Using saved answers from ${STATE_FILE} as defaults." >&2
}

saved() {
    # saved KEY: value from the last successful run, or empty.
    local v="SAVED_$1"
    printf '%s' "${!v:-}"
}

save_state() {
    local k tmp="${STATE_FILE}.tmp"
    for k in "${SAVED_KEYS[@]}"; do
        printf '%s=%s\n' "$k" "${!k:-}"
    done > "$tmp" && mv "$tmp" "$STATE_FILE"
    echo "Saved these settings to ${STATE_FILE} for the next run."
}

load_saved

ask() {
    # ask VAR_NAME "Prompt label" "default"
    local var="$1" label="$2" current="${!1:-}" reply
    local default; default=$(saved "$var"); default="${default:-$3}"
    if [[ -n "$current" ]]; then
        printf -v "$var" '%s' "$current"
        return
    fi
    read -r -p "${label} [${default}]: " reply </dev/tty
    printf -v "$var" '%s' "${reply:-$default}"
}

ask_required() {
    # ask_required VAR_NAME "Prompt label": keeps prompting until non-empty,
    # offering the saved value from the last successful run as the default.
    local var="$1" label="$2" current="${!1:-}" reply=""
    local default; default=$(saved "$var")
    if [[ -n "$current" ]]; then
        printf -v "$var" '%s' "$current"
        return
    fi
    while [[ -z "$reply" ]]; do
        if [[ -n "$default" ]]; then
            read -r -p "${label} [${default}]: " reply </dev/tty
            reply="${reply:-$default}"
        else
            read -r -p "${label}: " reply </dev/tty
        fi
    done
    printf -v "$var" '%s' "$reply"
}

create_application() {
    local name="" arch="" arch_choice
    while [[ -z "$name" ]]; do
        read -r -p "Application name: " name </dev/tty
    done

    echo "Architecture:" >&2
    echo "  1) X86_64" >&2
    echo "  2) ARM64" >&2
    read -r -p "Select architecture [1]: " arch_choice </dev/tty
    arch_choice="${arch_choice:-1}"
    case "$arch_choice" in
        1|X86_64|x86_64) arch="X86_64" ;;
        2|ARM64|arm64)   arch="ARM64" ;;
        *) echo "Invalid architecture choice." >&2; exit 1 ;;
    esac

    echo "Creating EMR Serverless application '${name}' (${arch}, emr-spark-8.1.0)..." >&2
    APPLICATION_ID=$(aws emr-serverless create-application \
        --release-label emr-spark-8.1.0 \
        --type SPARK \
        --name "$name" \
        --architecture "$arch" \
        --query 'applicationId' --output text)
    echo "Created application: ${APPLICATION_ID}" >&2
}

pick_application() {
    if [[ -n "${APPLICATION_ID:-}" ]]; then
        return
    fi

    echo "Fetching EMR Serverless applications..." >&2
    local tsv
    if ! tsv=$(aws emr-serverless list-applications \
        --query 'applications[].[id,name,state,architecture]' --output text 2>/dev/null); then
        echo "Could not list applications via AWS CLI." >&2
        read -r -p "Application ID: " APPLICATION_ID </dev/tty
        return
    fi

    local -a ids displays
    if [[ -n "$tsv" ]]; then
        while IFS=$'\t' read -r id name state arch; do
            ids+=("$id")
            displays+=("$(printf '%-30s [%-8s] %-7s %s' "$name" "$state" "$arch" "$id")")
        done <<< "$tsv"
    fi

    echo "Available applications:" >&2
    local i
    for i in "${!displays[@]}"; do
        printf '  %d) %s\n' $((i+1)) "${displays[i]}" >&2
    done
    local create_idx=$(( ${#ids[@]} + 1 ))
    printf '  %d) Create new application\n' "$create_idx" >&2

    local reply default_idx=1 last; last=$(saved APPLICATION_ID)
    for i in "${!ids[@]}"; do
        [[ "${ids[i]}" == "$last" ]] && default_idx=$((i+1))
    done
    read -r -p "Select application [${default_idx}]: " reply </dev/tty
    reply="${reply:-$default_idx}"
    if [[ "$reply" =~ ^[0-9]+$ ]] && (( reply >= 1 && reply <= ${#ids[@]} )); then
        APPLICATION_ID="${ids[$((reply-1))]}"
    elif [[ "$reply" =~ ^[0-9]+$ ]] && (( reply == create_idx )); then
        create_application
    else
        APPLICATION_ID="$reply"
    fi
}

pick_role() {
    if [[ -n "${JOB_ROLE_ARN:-}" ]]; then
        return
    fi

    # Only roles whose trust policy lets EMR Serverless assume them can run jobs.
    echo "Fetching IAM roles assumable by EMR Serverless..." >&2
    local tsv
    tsv=$(aws iam list-roles --output json 2>/dev/null | jq -r '
        .Roles[]
        | select([.AssumeRolePolicyDocument.Statement[]?
                  | select(.Effect == "Allow")
                  | .Principal.Service?
                  | if type == "array" then .[] else . end]
                 | index("emr-serverless.amazonaws.com"))
        | [.Arn, .RoleName] | @tsv') || tsv=""

    if [[ -z "$tsv" ]]; then
        echo "Could not list IAM roles via AWS CLI." >&2
        read -r -p "Execution role ARN: " JOB_ROLE_ARN </dev/tty
        return
    fi

    local -a arns displays
    while IFS=$'\t' read -r arn name; do
        arns+=("$arn")
        displays+=("$name")
    done <<< "$tsv"

    echo "Available IAM roles:" >&2
    local i
    for i in "${!displays[@]}"; do
        printf '  %d) %s\n' $((i+1)) "${displays[i]}" >&2
    done

    local reply default_idx=1 last; last=$(saved JOB_ROLE_ARN)
    for i in "${!arns[@]}"; do
        [[ "${arns[i]}" == "$last" ]] && default_idx=$((i+1))
    done
    read -r -p "Select role [${default_idx}]: " reply </dev/tty
    reply="${reply:-$default_idx}"
    if [[ "$reply" =~ ^[0-9]+$ ]] && (( reply >= 1 && reply <= ${#arns[@]} )); then
        JOB_ROLE_ARN="${arns[$((reply-1))]}"
    else
        JOB_ROLE_ARN="$reply"
    fi
}

pick_application
pick_role
ask_required JAR_S3    "JAR S3 path"
ask_required INPUT_S3  "Input  (CUR Parquet)"
ask_required OUTPUT_S3 "Output (enriched)"
ask PROVIDER       "Provider (AWS, GOOGLE, AZURE)" "$DEFAULT_PROVIDER"
ask FORMAT         "Report format (NATIVE, FOCUS)" "$DEFAULT_FORMAT"
ask JOB_NAME       "Job name"                "$DEFAULT_JOB_NAME"
ask EXEC_CORES     "spark.executor.cores"    "$DEFAULT_EXEC_CORES"
ask EXEC_MEMORY    "spark.executor.memory"   "$DEFAULT_EXEC_MEMORY"
ask EXEC_INSTANCES "spark.executor.instances" "$DEFAULT_EXEC_INSTANCES"
ask DRIVER_CORES   "spark.driver.cores"      "$DEFAULT_DRIVER_CORES"
ask DRIVER_MEMORY  "spark.driver.memory"     "$DEFAULT_DRIVER_MEMORY"
ask LOG_S3         "Logs S3 prefix ('-' = managed storage only)" "-"
[[ "$LOG_S3" == "-" ]] && LOG_S3=""
ask FOLLOW         "Follow progress (yes/no)" "$DEFAULT_FOLLOW"
ask POLL_INTERVAL  "Poll interval (s)"       "$DEFAULT_POLL_INTERVAL"

# Same checks as SparkJob, so a typo fails here rather than after EMR has spun up.
PROVIDER="${PROVIDER^^}"
FORMAT="${FORMAT^^}"
case "$PROVIDER" in
    AWS|GOOGLE|AZURE) ;;
    *) echo "ERROR: invalid provider '${PROVIDER}' (expected AWS, GOOGLE or AZURE)" >&2; exit 1 ;;
esac
case "$FORMAT" in
    NATIVE|FOCUS) ;;
    *) echo "ERROR: invalid report format '${FORMAT}' (expected NATIVE or FOCUS)" >&2; exit 1 ;;
esac
if [[ "$FORMAT" == "FOCUS" && "$PROVIDER" == "GOOGLE" ]]; then
    echo "ERROR: the FOCUS format is only supported for AWS and AZURE" >&2
    exit 1
fi

# Spark needs an explicit scheme; "bucket/key" would resolve to the default FS.
s3_uri() { [[ "$1" == s3://* || "$1" == s3a://* ]] && echo "$1" || echo "s3://$1"; }
JAR_S3=$(s3_uri "$JAR_S3")
INPUT_S3=$(s3_uri "$INPUT_S3")
OUTPUT_S3=$(s3_uri "$OUTPUT_S3")
[[ -n "$LOG_S3" ]] && LOG_S3=$(s3_uri "$LOG_S3")

# --- verify inputs ----------------------------------------------------------
verify_s3() {
    # verify_s3 "label" "s3://path": fails if the path doesn't exist or is empty.
    local label="$1" path="$2" out
    if ! out=$(aws s3 ls "$path" 2>/dev/null) || [[ -z "$out" ]]; then
        echo "ERROR: ${label} not found or empty at ${path}" >&2
        exit 1
    fi
}

echo "Verifying inputs on S3..."
verify_s3 "JAR"   "$JAR_S3"
verify_s3 "Input" "$INPUT_S3"

# --- submit -----------------------------------------------------------------
# EMR Serverless (emr-spark-8.x) runs Java 17, so the SPRUCE jar must target
# Java 17. Set EMR_JAVA_HOME only to force another JVM present in the image.
spark_params=""
if [[ -n "${EMR_JAVA_HOME:-}" ]]; then
    spark_params+=" --conf spark.emr-serverless.driverEnv.JAVA_HOME=${EMR_JAVA_HOME}"
    spark_params+=" --conf spark.executorEnv.JAVA_HOME=${EMR_JAVA_HOME}"
fi
spark_params+=" --conf spark.executor.cores=${EXEC_CORES}"
spark_params+=" --conf spark.executor.memory=${EXEC_MEMORY}"
spark_params+=" --conf spark.executor.instances=${EXEC_INSTANCES}"
spark_params+=" --conf spark.driver.cores=${DRIVER_CORES}"
spark_params+=" --conf spark.driver.memory=${DRIVER_MEMORY}"

job_driver=$(cat <<EOF
{
  "sparkSubmit": {
    "entryPoint": "${JAR_S3}",
    "entryPointArguments": ["-i", "${INPUT_S3}", "-o", "${OUTPUT_S3}", "-p", "${PROVIDER}", "-f", "${FORMAT}"],
    "sparkSubmitParameters": "${spark_params}"
  }
}
EOF
)

echo
echo "Submitting job '${JOB_NAME}' (${PROVIDER}, ${FORMAT}) to application ${APPLICATION_ID}..."
start_time=$SECONDS
jobRunId=$(aws emr-serverless start-job-run \
    --output text --query 'jobRunId' \
    --application-id "$APPLICATION_ID" \
    --execution-role-arn "$JOB_ROLE_ARN" \
    --name "$JOB_NAME" \
    --job-driver "$job_driver" \
    ${LOG_S3:+--configuration-overrides "{\"monitoringConfiguration\":{\"s3MonitoringConfiguration\":{\"logUri\":\"${LOG_S3}\"}}}"})

echo "jobRunId: ${jobRunId}"
echo "Inspect : aws emr-serverless get-job-run --application-id ${APPLICATION_ID} --job-run-id ${jobRunId}"

dashboard=$(aws emr-serverless get-dashboard-for-job-run \
    --application-id "$APPLICATION_ID" --job-run-id "$jobRunId" \
    --output text --query 'url' 2>/dev/null || true)
[[ -n "$dashboard" ]] && echo "Dashboard: ${dashboard}"

if [[ "${FOLLOW,,}" != "yes" && "${FOLLOW,,}" != "y" ]]; then
    exit 0
fi

# --- follow -----------------------------------------------------------------
echo
echo "Polling every ${POLL_INTERVAL}s. Ctrl-C to stop following (job keeps running)."
trap 'echo; echo "Detached. Job ${jobRunId} continues. Re-attach with: aws emr-serverless get-job-run --application-id ${APPLICATION_ID} --job-run-id ${jobRunId}"; exit 0' INT

last_state=""
while :; do
    read -r state details < <(aws emr-serverless get-job-run \
        --application-id "$APPLICATION_ID" --job-run-id "$jobRunId" \
        --output text --query 'jobRun.[state, stateDetails]')

    if [[ "$state" != "$last_state" ]]; then
        printf '[%s] %-12s %s\n' "$(date +%H:%M:%S)" "$state" "${details:-}"
        last_state="$state"
    else
        printf '[%s] %s\n' "$(date +%H:%M:%S)" "$state"
    fi

    case "$state" in
        SUCCESS)
            elapsed=$((SECONDS - start_time))
            printf 'Job completed successfully in %02d:%02d:%02d (h:m:s).\n' \
                $((elapsed/3600)) $(((elapsed%3600)/60)) $((elapsed%60))
            save_state
            exit 0
            ;;
        FAILED|CANCELLED)
            elapsed=$((SECONDS - start_time))
            echo
            printf 'Job ended in state %s after %02d:%02d:%02d (h:m:s).\n' \
                "$state" $((elapsed/3600)) $(((elapsed%3600)/60)) $((elapsed%60))
            aws emr-serverless get-job-run \
                --application-id "$APPLICATION_ID" --job-run-id "$jobRunId" \
                --query 'jobRun.{state:state, details:stateDetails, attempt:attempt, networkConfiguration:networkConfiguration}' \
                --output json || true
            if [[ -n "$LOG_S3" ]]; then
                echo "Driver stderr:"
                echo "  aws s3 cp ${LOG_S3%/}/applications/${APPLICATION_ID}/jobs/${jobRunId}/SPARK_DRIVER/stderr.gz - | gunzip | tail -50"
            fi
            exit 1
            ;;
    esac

    sleep "$POLL_INTERVAL"
done
