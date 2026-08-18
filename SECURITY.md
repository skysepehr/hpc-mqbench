# Security Policy

## Reporting A Vulnerability

Please report suspected vulnerabilities privately through the repository's
GitHub Security Advisory page. Do not open a public issue containing an
exploit, credential, private hostname, internal address, or cluster detail.

Include the affected command or component, impact, reproduction steps that do
not expose private data, and any suggested mitigation. Maintainers should
acknowledge a report before discussing public disclosure.

## Credential Policy

This project does not require credentials in source files. Supply Slurm account,
partition, QoS, module, interface, and storage settings through environment
variables or command options. Never commit:

- SSH private keys or agent sockets.
- Passwords, API keys, access tokens, or cloud credentials.
- `.env` files or generated `.local/hpc_env.sh` files.
- Kerberos tickets, proxy certificates, or kubeconfig files.
- Private registry credentials or authenticated Git remote URLs.

If a secret is committed, revoke or rotate it first. Removing it from the
latest commit is not sufficient; create a clean history or rewrite every
affected ref before sharing the repository.

## Operational Data

Raw benchmark outputs can contain node names, private IP addresses, scheduler
job IDs, usernames, filesystem paths, module inventories, and hardware details.
Treat these artifacts as potentially sensitive. The source-only release tool
excludes results, reports, Git history, runtime installations, and downloaded
third-party binaries.

## Network Exposure

Default Kafka and Pulsar benchmark profiles use plaintext test traffic and are
intended for isolated HPC allocations. Do not expose broker, Pulsar, Prometheus,
JMX, or exporter ports to an untrusted network. Add site-approved authentication
and encryption before adapting the suite to a shared or externally reachable
environment.

## Pre-Publication Checklist

1. Run `./benchmark.sh check`.
2. Generate a fresh allowlisted snapshot with
   `scripts/prepare_public_repository.py`.
3. Review `SOURCE_MANIFEST.json` and verify `SHA256SUMS`.
4. Initialize a new Git repository inside the snapshot; do not reuse the
   working repository's historical `.git` directory.
5. Create the GitHub repository as private and review its file list there.
6. Enable secret scanning and push protection when available.
7. Make the repository public only after a second reviewer approves it.
