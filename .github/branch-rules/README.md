# Branch Protection Rules

These JSON files define branch protection rules for the RoboSystems Service repository.

## Branch Types and Protection

### Main Branch (`main.json`)

- **Protected from deletion**: Cannot be deleted
- **Requires PR**: All changes must go through pull request
- **Status checks**: Tests must pass before merge
- **No force push**: History is preserved
- **Creation protection**: Only authorized users can create

### Release Branches (`release.json`)

- **Pattern**: `release/*`
- **Protected from deletion**: Release artifacts are permanent
- **No force push**: Preserve deployment history
- **Creation protection**: Only authorized users can create releases
- **Update protection**: Limited updates after creation

### Feature Branches (`feature.json`)

- **Pattern**: `feature/*`
- **Basic protection**: No force push, no deletion
- **Flexible**: Developers can work freely

### Bugfix/Hotfix Branches (`bugfix-hotfix.json`)

- **Pattern**: `bugfix/*`, `hotfix/*`
- **Basic protection**: No force push, no deletion
- **Flexible**: Quick fixes can be made

## Applying Rules

To apply these rules via GitHub API:

```bash
# Using GitHub CLI
gh api /repos/YourGitHubOrg/robosystems/rulesets \
  --method POST \
  --input branch-rules/main.json

gh api /repos/YourGitHubOrg/robosystems/rulesets \
  --method POST \
  --input branch-rules/release.json

gh api /repos/YourGitHubOrg/robosystems/rulesets \
  --method POST \
  --input branch-rules/feature.json

gh api /repos/YourGitHubOrg/robosystems/rulesets \
  --method POST \
  --input branch-rules/bugfix-hotfix.json
```

## Bypass Actors

All rules include bypass for:

- Organization Admins
- Repository Role ID 5 (likely "Maintain" or "Admin" role)

## Key Protections by Branch Type

| Branch Type | Deletion | Force Push | PR Required | Tests Required | Creation Control |
| ----------- | -------- | ---------- | ----------- | -------------- | ---------------- |
| main        | ✅       | ✅         | ✅          | ✅             | ✅               |
| release/\*  | ✅       | ✅         | ❌          | ❌             | ✅               |
| feature/\*  | ✅       | ✅         | ❌          | ❌             | ❌               |
| bugfix/\*   | ✅       | ✅         | ❌          | ❌             | ❌               |
| hotfix/\*   | ✅       | ✅         | ❌          | ❌             | ❌               |

## Notes

- Release branches are treated as permanent artifacts
- Only main requires PR reviews and status checks
- Feature/bugfix/hotfix branches have minimal protection for flexibility
- Organization admins can always bypass rules for emergency situations
