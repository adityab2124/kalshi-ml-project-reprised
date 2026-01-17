#!/usr/bin/env python3
"""
Slack notification helper for Kalshi pipeline.

Push-only notifications - no events, sockets, or interactivity.
"""

import os
from typing import Optional

try:
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError
    SLACK_AVAILABLE = True
except ImportError:
    SLACK_AVAILABLE = False
    WebClient = None
    SlackApiError = None


def send_slack_message(text: str, channel: Optional[str] = None) -> bool:
    """
    Send a Slack message.
    
    Args:
        text: Message text to send
        channel: Channel to send to. Can be:
            - Channel name: "#general" or "#channel-name"
            - User ID: "U0123ABCD" (for DMs)
            - Channel ID: "C0123ABCD"
            - If None, uses SLACK_CHANNEL env var, or defaults to "#general"
    
    Returns:
        True if message sent successfully, False otherwise
    """
    if not SLACK_AVAILABLE:
        print("⚠️  Warning: slack-sdk not installed. Install with: pip install slack-sdk")
        return False
    
    # Get Slack bot token from environment
    slack_token = os.getenv('SLACK_BOT_TOKEN')
    if not slack_token:
        print("⚠️  Warning: SLACK_BOT_TOKEN environment variable not set. Skipping Slack notification.")
        return False
    
    # Get channel from parameter, env var, or default
    if channel is None:
        channel = os.getenv('SLACK_CHANNEL', '#general')
    
    try:
        client = WebClient(token=slack_token)
        response = client.chat_postMessage(
            channel=channel,
            text=text
        )
        
        if response["ok"]:
            print(f"✓ Slack message sent to {channel}")
            return True
        else:
            print(f"⚠️  Warning: Slack API returned error: {response.get('error', 'Unknown error')}")
            return False
            
    except SlackApiError as e:
        error_code = e.response.get('error', 'unknown_error')
        error_msg = f"⚠️  Warning: Slack API error: {error_code}"
        
        if error_code == 'channel_not_found':
            error_msg += (
                "\n   Channel not found. To fix:\n"
                "   1. For a channel: Use '#channel-name' (e.g., '#general')\n"
                "   2. For a DM: Use your Slack user ID (e.g., 'U0123ABCD')\n"
                "      Find your user ID: https://api.slack.com/methods/users.identity\n"
                "   3. Set SLACK_CHANNEL env var: export SLACK_CHANNEL='#your-channel'\n"
                "   4. Or pass channel directly: send_slack_message('text', channel='#general')"
            )
        print(error_msg)
        return False
    except Exception as e:
        print(f"⚠️  Warning: Failed to send Slack message: {e}")
        return False
