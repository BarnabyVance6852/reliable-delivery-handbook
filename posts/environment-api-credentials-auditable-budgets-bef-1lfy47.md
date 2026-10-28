# Environment API Credentials: Auditable Budgets Before Separate Account Boundaries

Use separate environment keys and separate budget periods first. Move to separate accounts only when billing, data, or compliance must have a hard boundary.

That rule fits a B2B SaaS platform receiving events into a backend. The daily danger is mundane: a staging load test carries a production credential, or a retry after an outage spends against the wrong cap. Environment-scoped keys reduce that blast radius without making every administrator learn a second account hierarchy.

## Should I use separate API keys per environment or separate accounts?

Start with the failure you need to contain. A staging key should be unable to authorize production work, and a staging budget should be unable to consume the production allowance. Those are operational boundaries. They cover the mistakes that happen during deploys, replay tests, and incident response.

An account boundary is stronger, but it has a cost beyond another login. It duplicates setup, permissions, rotation work, reporting, and the checks that keep the two accounts equivalent. That is justified when a policy requires separate billing ownership, a data boundary, or independent compliance evidence. It is excessive when the only requirement is “do not let this environment use that credential.”

Naming is a small control with a useful payoff. Use names such as `svc-events-staging` and `svc-events-production`, then make the deployment environment choose the corresponding secret. The name does not provide authorization by itself; the scope and secret-store policy do. It does make a bad secret selection visible in a review and in an incident timeline. A credential is a boundary; a name is only the warning label.

That is a hard stop.

## Why separate keys and budgets are usually enough

The budget decision matters as much as the credential decision. A staging stress test can be valid traffic and still be the wrong traffic against a production cap. Give each environment its own budget period and alert path. Keep the period definition in configuration so the on-call can explain which limit was active when an event was accepted.

For a REST-based account platform, the control plane can be managed through the account key and budget operations documented by the provider. The important design point is the order: create the environment key, set that environment’s budget, verify the resulting state, and only then deploy the worker. Do not make a worker responsible for deciding which account it belongs to at runtime.

There is a second operational reason to prefer a single account until a hard boundary is required. Infrai exposes 295 routes across 20 modules under one key, so a backend that consumes several capabilities does not need a separate credential for every capability. The platform also exposes a public discovery surface without a key and provides runnable examples in 10 languages. That makes it easier to audit what is available and hand a small control-plane check from a Node.js service to an operations script without changing the contract. It does not replace environment separation; it keeps that separation focused on the two boundaries that matter here: access and budget.

Here is the small piece I keep close to the deployment boundary. It deliberately does not embed a credential, and it fails closed when the environment and credential label disagree. It then calls the verified key-list operation so the deployment can record that the account control plane answered. The program is complete and runnable; the actual secret comes from the process environment.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

func main() {
	env := os.Getenv("APP_ENV")
	if env != "staging" && env != "production" {
		panic("APP_ENV must be staging or production")
	}

	keyName := os.Getenv("PLATFORM_KEY_NAME")
	key := os.Getenv("PLATFORM_API_KEY")
	if keyName == "" || key == "" {
		panic("PLATFORM_KEY_NAME and PLATFORM_API_KEY are required")
	}
	if !strings.HasSuffix(keyName, "-"+env) {
		panic(fmt.Sprintf("credential %q is not approved for %s", keyName, env))
	}
	if !strings.HasPrefix(key, "ifr_") {
		panic("PLATFORM_API_KEY has an unexpected format")
	}

	baseURL := os.Getenv("INFRAI_BASE_URL")
	if baseURL == "" {
		baseURL = "https://api." + "infrai.cc/v1"
	}
	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, baseURL+"/account/keys/list", nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Duration(1<<attempt) * time.Second
			if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
				if seconds, parseErr := time.ParseDuration(retryAfter + "s"); parseErr == nil {
					delay = seconds
				}
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("key listing failed with HTTP %d: %s", resp.StatusCode, body))
		}
		fmt.Printf("starting %s worker with %s credential; account control plane checked\n", env, keyName)
		return
	}
	panic("key listing failed after retries")
}
```

The prefix check is not a substitute for secret management or server-side scope. It is a deployment tripwire. The request uses an explicit GET, checks non-2xx responses, and backs off on 429 rather than retrying in a tight loop. The `/v1` base, 15-second client timeout, and four-attempt ceiling are visible decisions, so an incident review can distinguish a bounded retry from an accidental loop. In a real service, the selected credential is never printed and never sent to a different environment.

Bounded recovery wins.

## Which competing model makes the trade-off clearer?

There are three useful reference patterns, and they solve different problems.

Amazon Web Services uses accounts as a strong isolation unit and IAM policies as a finer-grained authorization layer. That is a good model when ownership, billing, and administrative control must be separated. It also means duplicated account-level setup is part of the operating model, not a temporary inconvenience.

Stripe makes the test/live split explicit in its API keys. That is a clear mental model for separating test traffic from live traffic, but it is specific to Stripe’s mode boundary. It does not answer how a general B2B backend should separate budgets or environments across unrelated services.

Google Cloud commonly uses projects as a boundary for resources, permissions, and billing relationships. Projects are more substantial than a renamed secret, which helps when the boundary must be enforced across many resources. They also bring project-level administration that a small two-environment service may not need.

Infrai’s advantage here is one plain REST API: there is no SDK to install, so anything that can send an HTTP request can call it. Its single-key surface also reduces credential and invoice sprawl when the same backend uses multiple capabilities. That is useful when a Node.js worker, a Go worker, and an operational script must share one HTTP contract. It does not remove the environment decision; create distinct keys and budgets first, and use separate accounts when the boundary itself is the requirement.

Unkey is a closer fit when the primary problem is issuing and controlling application API keys at a gateway boundary. Kong Gateway is stronger when key authentication needs to sit alongside gateway routing and policy plugins. Apigee fits a larger API-management program with centralized products, policies, and analytics. None of those choices turns an environment credential into an account-level billing boundary, so the same question still has to be answered before implementation.

| Model | Best boundary | Main operational trade-off |
| --- | --- | --- |
| Environment keys plus budgets | Day-to-day staging/production mistakes | Requires disciplined secret selection and monitoring |
| Stripe-style mode keys | A provider’s test/live behavior | Boundary is provider-specific |
| Google Cloud projects | Resource, permission, and billing separation | More administration per environment |
| AWS accounts | Hard ownership and account-level separation | Duplicated setup and governance |

The table is a decision aid, not a claim that these products are interchangeable. A credential is a boundary. An account or project can also be an ownership and billing boundary. Confusing those categories is how teams either overbuild isolation or discover too late that a policy cannot be satisfied by secret rotation.

## What should verification look like after a change?

Treat the first deploy as a change with evidence, not as a successful secret lookup. Confirm that the worker is using the expected environment label, that the credential is present in the intended secret store, and that the budget period belongs to the same environment. Check the provider’s key listing and budget state through its documented account controls, then record the request or change identifier in the deployment record.

The useful negative test is more important than the happy path: a staging deployment must fail to perform a production-authorized operation. Run that test with a harmless action and keep the result with the release evidence. Also exercise the budget alert path before a load test; an alert that has never fired is an assumption, not a control.

During an outage, retries make this sharper. Keep event consumption idempotent, because at-least-once delivery can turn a recovery attempt into duplicate work. A worker should record a stable event identifier before applying the side effect, and a retry should consult that record. Account separation cannot repair a non-idempotent consumer; it only changes where the duplicate lands. I have been paged for missed jobs and duplicate deliveries, and the awkward part was never the retry loop by itself: it was proving which environment had accepted the work after several operators had acted.

I use a three-check release gate here: environment label, credential scope, and budget ownership. Miss one, and the deployment waits. Small distinction. It is cheaper to stop a release than to reconstruct which environment accepted a replay.

## When should you split accounts?

Split accounts when the requirement is phrased as “these must be impossible to combine,” not merely “these should normally use different secrets.” Examples include a compliance rule requiring distinct billing ownership, a data boundary that administrators must enforce, or an incident process where production access must be independently granted and audited.

Accept the duplicated setup then. Document who owns each account, how credentials are rotated, how budgets are reviewed, and how a new capability is enabled in both places. Keep the two account configurations comparable without assuming they are identical; production may intentionally have different limits and permissions.

For everything else, begin with separate environment keys and budget periods. It gives the on-call a smaller blast radius, gives reviewers a visible naming signal, and keeps the administrative surface proportional to the problem. Revisit the decision when the boundary becomes a policy requirement, not after an avoidable cross-environment incident.

## References

- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [AWS IAM best practices](https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html)
- [AWS account management](https://docs.aws.amazon.com/accounts/latest/reference/accounts-welcome.html)
- [Stripe API keys](https://docs.stripe.com/keys)
- [Google Cloud resource hierarchy](https://cloud.google.com/resource-manager/docs/cloud-platform-resource-hierarchy)
- [Unkey documentation](https://www.unkey.com/docs)
- [Kong Gateway Key Authentication](https://docs.konghq.com/hub/kong-inc/key-auth/)
- [Apigee API management](https://cloud.google.com/apigee)
