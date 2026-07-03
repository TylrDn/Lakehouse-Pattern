# Security Policy

## Supported Versions

Only the latest minor release on the `main` branch receives security fixes.

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |
| < 0.1   | :x:                |

## Reporting a Vulnerability

If you believe you have found a security vulnerability in Lakehouse-Pattern,
please **do not open a public issue**. Instead, report it privately so the
issue can be triaged and patched before disclosure.

### Preferred channel

Use GitHub's private vulnerability reporting:

1. Navigate to the [Security tab](https://github.com/TylrDn/Lakehouse-Pattern/security)
   of this repository.
2. Click **Report a vulnerability**.
3. Fill in a clear description, reproduction steps, and impact assessment.

### What to include

- A description of the issue and the potential impact.
- Steps to reproduce, including affected commit SHA if possible.
- Any suggested mitigations you are aware of.
- Your name / handle for credit (optional).

### What to expect

- **Acknowledgement** within 3 business days.
- **Triage and severity assessment** within 7 business days.
- **Fix or mitigation timeline** communicated after triage. Critical issues
  are prioritized; low-severity issues may be batched into the next release.
- **Public disclosure** coordinated with you after a fix ships.

## Scope

In scope:

- Code under `lakehouse/`, `ingestion/`, `transform/`, `pipelines/`,
  `orchestration/`, `ml/`, `serving/`, and CI workflows.
- Default configuration and documented usage paths.

Out of scope:

- Vulnerabilities in third-party dependencies (report to the upstream project;
  Dependabot will surface CVEs here automatically).
- Denial-of-service via obviously oversized inputs to the sample dataset.
- Issues that require an already-compromised host or credentials.
