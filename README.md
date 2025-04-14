# Tripo MCP Server

Tripo MCP provides an interface between AI assistants and [Tripo AI](https://www.tripo3d.ai) via [Model Context Protocol (MCP)](https://github.com/anthropics/anthropic-cookbook/tree/main/mcp). 

> **Note:** This project is in alpha. Currently, it supports Tripo Blender Addon integration.

## Current Features

- Generate 3D asset from natural language using Tripo's API and import to Blender
- Compatible with Claude and other MCP-enabled AI assistants

## Quick Start

### Prerequisites
- Python 3.10+
- [Blender](https://www.blender.org/download/)
- [Tripo AI Blender Addon](https://www.tripo3d.ai/app/home)
- Claude for Desktop or Cursor IDE

### Installation

1. Install Tripo AI Blender Addon from [Tripo AI's website](https://www.tripo3d.ai/app/home)

2. Configure the MCP server in Claude Desktop or Cursor.

    * `pip install uv`
    * set mcp in cursor
    ```json
    {
      "mcpServers": {
        "tripo-mcp": {
          "command": "uvx",
          "args": [
            "tripo-mcp"
          ]
        }
      }
    }
    ```

    * Then you will get a green dot like this:
      ![img](succeed.jpg)

### Usage

1. Enable Tripo AI Blender Addon and start blender mcp server.

2. Chat using cursor or claude. E.g., "Generate a 3D model of a futuristic chair".

## Acknowledgements

- **[Tripo AI](https://www.tripo3d.ai)**
- **[blender-mcp](https://github.com/ahujasid/blender-mcp)** by [Siddharth Ahuja](https://github.com/ahujasid)

**Special Thanks**  
Special thanks to Siddharth Ahuja for the blender-mcp project, which provided inspiring ideas for MCP + 3D.
