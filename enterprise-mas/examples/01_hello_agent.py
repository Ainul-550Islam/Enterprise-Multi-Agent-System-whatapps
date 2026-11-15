# 01_hello_agent.py

A simple "Hello, World!" agent that demonstrates the basics of creating
and running an agent via the mas-cli.

## Usage

```bash
# Bootstrap the organization
mas admin bootstrap   --organization org-1234   --tenant tenant-5678   --project project-9012   --owner owner-3456

# Create an agent
mas agent create   --name "hello-agent"   --scope tenant   --capabilities "chat"

# Run the agent
mas execution submit   --project project-9012   --workflow hello-workflow   --input '{"message": "Hello!"}'
```
