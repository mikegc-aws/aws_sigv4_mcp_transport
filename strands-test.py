import sys
import os

# Add parent directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from strands import Agent
from strands.models import BedrockModel
from strands.tools.mcp import MCPClient
from sigv4_mcp_transport import sigv4_mcp_transport

# Your AgentCore Gateway URL
server_url = "https://gateway-quick-start-ace411-9yjeldir1v.gateway.bedrock-agentcore.us-west-2.amazonaws.com/mcp"

# Create transport callable using the proper implementation
transport_callable = lambda: sigv4_mcp_transport(server_url)

# Use standard Strands MCPClient with our SigV4 transport
mcp_client = MCPClient(transport_callable)

# Create agent with Bedrock model
model = BedrockModel(model_id="anthropic.claude-3-5-sonnet-20241022-v2:0", region_name="us-west-2")

print("🤖 Strands Agent with Proper SigV4 Transport")
print("=" * 50)

# Use MCP client with context manager
with mcp_client:
    tools = mcp_client.list_tools_sync()
    print(f"✅ Found {len(tools)} tools!")
    for tool in tools:
        print(f"   - {tool.tool_name}: {tool.tool_spec['description']}")
    
    # Create agent with MCP tools
    agent = Agent(model=model, tools=tools)
    
    print(f"\n💬 Chat with the agent! (Type 'quit' to exit)")
    print("-" * 50)
    
    while True:
        user_input = input("\n👤 You: ")
        if user_input.lower() in ['quit', 'exit', 'bye']:
            break
        response = agent(user_input)
        print(f"🤖 Agent: {response}")
