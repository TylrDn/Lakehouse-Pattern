# Delta Sharing — OSS server + client demo

Delta Sharing is an open protocol; Databricks provides a managed server,
but the reference implementation is open-source and ships in the
`delta-sharing` PyPI package.

## What this directory ships

* `config.json` — a self-hostable Delta Sharing server config that exposes
  `main.gold.daily_revenue` and `main.gold.customer_ltv` as a share.
* `bearer_tokens.json` — mock recipient tokens (real deployments use JWT).
* `client_demo.py` — a `delta-sharing` client that reads the share and
  prints the first N rows. This is what a *recipient* runs — no Spark
  required.

## Databricks-native mapping

| Databricks | This repo |
| --- | --- |
| `CREATE SHARE` + `ALTER SHARE ... ADD TABLE` | `config.json` entries |
| `CREATE RECIPIENT` | `bearer_tokens.json` |
| Managed sharing server on the workspace | `delta-sharing-server` (OSS jar) |
| Recipient sends the token to Excel/Tableau/PowerBI | Same — the OSS server speaks the identical REST protocol |

## Run

```bash
# Server (needs Java 17 + the reference jar from the delta.io/sharing release page)
java -jar delta-sharing-server-1.0.5.jar --config sharing/config.json

# Client
python -m sharing.client_demo --profile sharing/recipient.share --limit 20
```
