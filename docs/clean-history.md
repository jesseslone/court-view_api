# Clean History / New Repository

This project can be fully sanitized by creating a new repository from the current cleaned working tree.

## Recommended (safest): New repo from sanitized snapshot

From parent directory:

```bash
cd /path/to/workspaces
rm -rf courtview_lookup_clean
mkdir courtview_lookup_clean
rsync -a --exclude='.git' courtview_lookup/ courtview_lookup_clean/
cd courtview_lookup_clean
git init
git add .
git commit -m "Initial sanitized CourtView API service"
# create new remote repo, then:
git remote add origin <NEW_REMOTE_URL>
git branch -M main
git push -u origin main
```

Do not push the old repository history.

## If you must keep same repo name (history rewrite)

Use [`git filter-repo`](https://github.com/newren/git-filter-repo) to rewrite all commits and force-push.

High level:

1. Backup repository
2. Run filter-repo to remove/replace sensitive terms and sensitive files
3. Force-push rewritten branches/tags
4. Ask collaborators to re-clone

This is disruptive and should be coordinated.
