# Guidelines for Contributors

Thank you for your interest in contributing to Zorse. This document explains our contribution process and procedures.

## Sign all of your git commits!
All contributions must align with the [Open Mainframe Project contribution guidelines](https://github.com/openmainframeproject/tac/blob/master/process/contribution_guidelines.md), including having a DCO signoff on all commits.

Whenever you make a commit, it is required to be signed. If you do not, you will have to re-write the git history to get all commits signed before they can be merged, which can be quite a pain.

Use the "-s" or "--signoff" flags to sign a commit.

Example calls:
* `git commit -s -m "Adding a test file to new_branch"`
* `git commit --signoff -m "Adding a test file to new_branch"`

Why? Sign-off is a line at the end of the commit message which certifies who is the author of the commit. Its main purpose is to improve tracking of who did what, especially with patches.

Example commit in git history:

```
Add tests for the payment processor.

Signed-off-by: Humpty Dumpty <humpty.dumpty@example.com>
```

What to do if you forget to sign off on a commit?

To sign old commits: `git rebase --exec 'git commit --amend --no-edit --signoff' -i <commit-hash>`

where commit hash is one before your first commit in history

If you are committing via the GitHub UI directly, check out these [useful tools](https://github.com/openmainframeproject/tac/blob/main/process/contribution_guidelines.md#useful-tools-to-make-doing-dco-signoffs-easier).


## Technical Steering Committee (TSC)

Zorse is governed by a Technical Steering Committee (TSC). Its role is to:

- Set the overall direction of the Zorse
- Ensure the Zorse community has the needed resources and infrastructure to succeed
- Resolve any issues within the Zorse community
- Provide Zorse updates to the TAC and the community at large

### Members

| Name     | Surname | Email |
|------------|-------|-------|
| Gabriel    | Gordon-Hall  | gabriel@bloop.ai  |
