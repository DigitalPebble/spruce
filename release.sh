#!/bin/bash
set -e

# Extract current version from pom.xml (strips -SNAPSHOT if present)
CURRENT_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout | sed 's/-SNAPSHOT//')

echo "Current POM version: $CURRENT_VERSION"
echo ""

# Ask for release version
read -rp "Release version [$CURRENT_VERSION]: " RELEASE_VERSION
RELEASE_VERSION=${RELEASE_VERSION:-$CURRENT_VERSION}

# Suggest next snapshot version (bumps minor version)
MAJOR=$(echo "$RELEASE_VERSION" | cut -d. -f1)
MINOR=$(echo "$RELEASE_VERSION" | cut -d. -f2)
NEXT_MINOR=$((MINOR + 1))
SUGGESTED_NEXT="$MAJOR.$NEXT_MINOR"

read -rp "Next development version (without -SNAPSHOT) [$SUGGESTED_NEXT]: " NEXT_VERSION
NEXT_VERSION=${NEXT_VERSION:-$SUGGESTED_NEXT}

echo ""
echo "========================================="
echo "  Release version : $RELEASE_VERSION"
echo "  Next dev version: $NEXT_VERSION-SNAPSHOT"
echo "========================================="
read -rp "Proceed? [y/N]: " CONFIRM
[[ "$CONFIRM" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 1; }

echo ""
echo "--- Setting version to $RELEASE_VERSION ---"
mvn versions:set -DnewVersion="$RELEASE_VERSION"

echo "--- Committing and tagging ---"
git commit -sam "Release $RELEASE_VERSION"
git push
git tag "$RELEASE_VERSION"
git push origin "$RELEASE_VERSION"

echo "--- Building ---"
mvn clean package

echo "--- Creating GitHub release ---"
gh release create "$RELEASE_VERSION" --generate-notes "./target/spruce-$RELEASE_VERSION.jar"

echo "--- Setting next development version ---"
mvn versions:set -DnewVersion="$NEXT_VERSION-SNAPSHOT"
git commit -sam "Post release $RELEASE_VERSION"
git push

echo "--- Ensuring gh-milestone extension is installed ---"
gh extension list | grep -q "gh-milestone" || gh extension install valeriobelli/gh-milestone

echo "--- Managing milestones ---"
MILESTONE_NUMBER=$(gh milestone list --json number,title --jq ".[] | select(.title == \"$RELEASE_VERSION\") | .number")
if [ -n "$MILESTONE_NUMBER" ]; then
  gh api --method PATCH "repos/:owner/:repo/milestones/$MILESTONE_NUMBER" -f state=closed
  echo "Closed milestone $RELEASE_VERSION (#$MILESTONE_NUMBER)"
else
  echo "Warning: milestone '$RELEASE_VERSION' not found, skipping close"
fi
gh milestone create --title "$NEXT_VERSION"

echo ""
echo "Done! Released $RELEASE_VERSION, now on $NEXT_VERSION-SNAPSHOT."
