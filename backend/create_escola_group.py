import sys
import asyncio
import json
import uuid
import time
from sqlalchemy import select, update

sys.path.append('/home/kaauan/projects/open-webui/backend')

from open_webui.models.groups import Groups, GroupForm, Group, GroupModel
from open_webui.models.users import Users
from open_webui.internal.db import get_async_db_context

async def main():
    permissions = {
        'workspace': {
            'models': False,
            'knowledge': False,
            'prompts': False,
            'tools': False,
            'skills': False,
            'models_import': False,
            'models_export': False,
            'prompts_import': False,
            'prompts_export': False,
            'tools_import': False,
            'tools_export': False,
        },
        'sharing': {},
        'chat': {
            'controls': False,
            'valves': False,
            'system_prompt': False,
            'params': False,
            'file_upload': False,
            'web_upload': False,
            'delete': False,
            'delete_message': False,
            'continue_response': False,
            'regenerate_response': False,
            'rate_response': False,
            'edit': False,
            'share': False,
            'export': False,
            'stt': False,
            'tts': False,
            'call': False,
            'multiple_models': False,
            'temporary': False,
            'temporary_enforced': False,
        },
        'features': {
            'api_keys': False,
            'notes': False,
            'folders': False,
            'channels': False,
            'direct_tool_servers': False,
            'web_search': False,
            'image_generation': False,
            'code_interpreter': False,
            'memories': False,
            'automations': False,
            'calendar': False,
        },
        'settings': {
            'interface': False,
        }
    }

    # First, let's check if the group already exists
    existing_group = await Groups.get_group_by_name("escola")
    if existing_group:
        print(f"Group 'escola' already exists. ID: {existing_group.id}. Updating permissions.")
        # Updating group manually
        async with get_async_db_context() as db:
            await db.execute(
                update(Group)
                .filter_by(id=existing_group.id)
                .values(permissions=permissions, updated_at=int(time.time()))
            )
            await db.commit()
            print("Permissions updated.")
        return

    # Find an admin user
    users_resp = await Users.get_users(0, 100)
    users = users_resp.get("items", [])
    admin_id = users[0].id if users else 'admin'

    form = GroupForm(
        name="escola",
        description="Grupo restrito para estudantes com interface minima",
        permissions=permissions
    )

    group = await Groups.insert_new_group(admin_id, form)
    if group:
        print(f"Group 'escola' created with ID: {group.id}")
    else:
        print("Failed to create group")

if __name__ == '__main__':
    asyncio.run(main())
