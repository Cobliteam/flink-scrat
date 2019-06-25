#!/bin/bash

set -ex -o pipefail

# Check pre-conditions

if ! [ -f .bumpversion.cfg ]; then
  echo "No bumpversion config found, not releasing."
  exit 0
fi

if [[ -n "$TRAVIS_PULL_REQUEST" && "$TRAVIS_PULL_REQUEST" != false ]]; then
  echo "In a pull request build, not releasing"
  exit 0
fi

if [[ -n "$TRAVIS_TAG" ]]; then
  echo "In a tag build, not releasing"
  exit 0
fi

if [ -z "$GITHUB_TOKEN" ]; then
  echo "Error: GITHUB_TOKEN not set"
  exit 1
fi

if git show --name-only --pretty='format:' HEAD | grep -qF .bumpversion.cfg; then
  echo "Version manually bumped, not releasing"
  exit 0
fi

if [ -z "$BUMPVERSION_PRERELEASE_PART" ]; then
  bumpversion_prerelease_part="patch"
else
  bumpversion_prerelease_part=$BUMPVERSION_PRERELEASE_PART
fi

if [ -z "$1" ]; then
  bumpversion_part="patch"
else
  bumpversion_part=$1
fi

# Configure git for pushing
git config --global user.email "admin@cobli.co"
git config --global user.name "Cobli CD"
git remote set-url --push origin "https://cobli-cd:${GITHUB_TOKEN}@github.com/${TRAVIS_REPO_SLUG}.git"
git checkout "$TRAVIS_BRANCH"

# Install Deps
# Forked githubrelease to fix a issue related to encoding (https://github.com/j0057/github-release/issues/40)
export PYTHONIOENCODING=utf8
[ -x venv ] || virtualenv venv
source venv/bin/activate
pip install "git+https://github.com/peritus/bumpversion.git" \
  "git+https://github.com/Cobliteam/github-release.git" \
  'urllib3[secure]'

# Fetch tags so bumpversion fails in case of duplicates
git fetch --tags

new_version=$(bumpversion -n --list $bumpversion_part \
  | grep new_version \
  | sed -E 's/new_version\s*=\s*//')

new_tag="v${new_version}"

# If in master bump release and set dev to the next non-production version
bumpversion $bumpversion_part \
  --commit --tag --tag-name='v{new_version}' \
  --message 'Release {new_version} [ci skip]'

dev_push=
if [[ "$TRAVIS_BRANCH" != dev ]] && git fetch origin dev:dev; then
    git checkout dev
    git merge --ff-only "$TRAVIS_BRANCH"
    bumpversion $bumpversion_prerelease_part \
        --commit --no-tag \
        --message 'Start dev version {new_version} [ci skip]'
    git checkout -

    dev_push=dev
fi

git push --atomic origin "$TRAVIS_BRANCH" "$new_tag" $dev_push

# Make the Github releases
gen_release_notes() {
    local last_tag=$(git describe --tags --abbrev=0 "${new_tag}^" | head -n1)
    local start_date=$(git log -1 --format='%ai' "$last_tag")
    local merges=$(git log --merges "${last_tag}..${new_tag}^" --format=$'  - %b' | sed -E '/^\s*$/d')
    if [ -n "$merges" ]; then
        echo "Merges since $start_date:"
        echo
        echo "$merges"
    else
        local shortlog=$(git shortlog "${last_tag}..${new_tag}^")
        echo "Changes since $start_date:"
        echo "<pre>"
        echo "$shortlog"
        echo "</pre>"
    fi
}

githubrelease release "$TRAVIS_REPO_SLUG" create "$new_tag"
githubrelease release "$TRAVIS_REPO_SLUG" edit "$new_tag" --body "$(gen_release_notes)"
githubrelease release "$TRAVIS_REPO_SLUG" publish "$new_tag"
