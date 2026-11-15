# 03_supervisor_team.py

A supervisor agent that delegates to a team of specialized agents.

## Usage

```bash
# Create specialized agents
mas agent create --name "researcher" --scope org --capabilities "search"
mas agent create --name "writer" --scope org --capabilities "composition"
mas agent create --name "reviewer" --scope org --capabilities "editing"

# Create a supervisor workflow
mas workflow create   --name "content-pipeline"   --definition '{"steps": ["research", "write", "review"]}'

# Run the pipeline
mas execution submit   --project project-9012   --workflow content-pipeline
```
