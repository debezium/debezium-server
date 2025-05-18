# Auto Update Debezium Fork Pipeline

## Overview
This GitHub Actions workflow automates the synchronization of a forked repository with its upstream repository, specifically for the `debezium/debezium-server` project. It runs on a weekly schedule and can also be triggered manually.

---

## Trigger
- **Scheduled:** Runs every Sunday at midnight (UTC):   
  `cron: '0 0 * * 0'`

---

#### Steps

1. **Checkout Main Branch**  
   Uses `actions/checkout@v2` to clone the main branch of the forked repository.  
   - Reference branch: `main`  
   - Authentication token: `${{ secrets.GITHUB_TOKEN }}`

2. **Synchronize Fork**  
   Uses `TobKed/github-forks-sync-action@master` to sync the fork with upstream.  
   - Authentication token: `${{ secrets.DEBEZIUM_WORKFLOW_TOKEN }}`
   - Upstream repository: `debezium/debezium-server`
   - Target repository (fork): `get-bridge/debezium-server`
   - Branches: both `main`
   - Options: `force` update enabled, tags synchronized

3. **Timestamp Output**  
   Runs `date` command to log the current timestamp indicating when the update occurred.

---

## Summary
This pipeline ensures that the forked repository remains up-to-date with the upstream `debezium/debezium-server` repository, running weekly and on demand, and logs each update with a timestamp.


## Sidenote:
Developer's of Debezium mentioned that is safe to sync main to main, highly unlikely that their main branch is not buildable. 