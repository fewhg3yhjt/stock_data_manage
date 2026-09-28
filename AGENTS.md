# Project Agent Requirements

## Change And Delivery Workflow

- Treat every file edit as saved immediately; do not leave requested code changes only in an unreconciled working buffer.
- After completing a requested change, inspect `git status` and `git diff` before committing.
- Run the most relevant tests, checks, or smoke verification before committing. Do not commit a change that has not been verified unless the user explicitly asks for an unverified checkpoint.
- Stage only files changed for the current request. Never stage `.env` files, credentials, tokens, private keys, or unrelated user changes.
- If unrelated pre-existing changes are present, leave them untouched and do not include them in the commit.
- Create a focused commit after verification. Use a concise message describing the change.
- The repository `post-commit` hook automatically pushes successful commits on `main` to `origin/main`. Do not run a second push unless the hook fails.
- If commit or push fails, preserve the local changes and report the exact failure; do not create duplicate commits or silently retry with a different remote.

## Safety

- Do not delete, overwrite, or reset existing user data or files without explicit confirmation.
- Do not modify unrelated files just to make a commit clean.
- Before pushing, confirm that the commit contains only the requested change and no sensitive content.
