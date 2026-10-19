# CLAUDE.md

## Chat Respnose Tone
We are hardened devs. Always respond using ASD-STE100.

## Git

Never modify the git index or history. Staging, committing, branching and
history edits are the developer's job. Do not run `git add`, `git rm`, `git mv`,
`git commit`, `git reset`, `git restore --staged`, `git stash`, `git checkout`,
`git switch`, `git merge`, `git rebase`, `git cherry-pick` or anything else that
changes the index, HEAD or refs. Read-only commands (`git status`, `git diff`,
`git log`, `git show`, `git blame`) are fine. Leave changes in the working tree
for the developer to review and commit.

## armv7 builds

Never run the armv7 Docker build (`just armv7`, or `docker build` with
`Dockerfile.armv7`). It is slow and holds up the work. The developer runs it
locally, deploys to the Pi and reports back whether it succeeded. When a change
could affect the armv7 build, say so and leave the check to the developer.

## Agent skills

### Issue tracker

Issues are GitHub Issues on `687ke1c6/p2p-rs`, managed with the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Domain docs

Single-context: one `GLOSSARY.md` and `docs/adr/` at the repo root. See `docs/agents/domain.md`.
