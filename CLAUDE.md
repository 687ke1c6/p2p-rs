# CLAUDE.md

## Git

Never modify the git index or history. Staging, committing, branching and
history edits are the developer's job. Do not run `git add`, `git rm`, `git mv`,
`git commit`, `git reset`, `git restore --staged`, `git stash`, `git checkout`,
`git switch`, `git merge`, `git rebase`, `git cherry-pick` or anything else that
changes the index, HEAD or refs. Read-only commands (`git status`, `git diff`,
`git log`, `git show`, `git blame`) are fine. Leave changes in the working tree
for the developer to review and commit.
