Review all staged changes (`git diff --cached`) with focus on three contexts:

## Application Context (robosystems/)

- Does the code follow existing patterns in the codebase?
- Are there security concerns (SQL injection, auth bypass, data exposure)?
- Is error handling appropriate?
- Are credits/billing changes correct?
- Do graph operations respect multi-tenancy?

## Infrastructure Context (cloudformation/)

- Are IAM permissions least-privilege?
- Are there hardcoded values that should be parameters?
- Will this change cause resource replacement vs update?
- Are secrets handled via Secrets Manager (never hardcoded)?
- Is the template valid? (`just cf-lint <template>`)

## Deployment Context (.github/workflows/)

- Are environment variables and secrets correctly referenced?
- Is the deployment order correct (dependencies first)?
- Are there race conditions or timing issues?
- Do staging and production configurations align appropriately?

## Output

Provide a summary with:
1. **Issues**: Problems that should be fixed before commit
2. **Suggestions**: Improvements that aren't blocking
3. **Questions**: Anything unclear that needs clarification
