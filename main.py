import requests
import json
from settings import *

def get_elk_data(action, from_time, to_time, index="_all"):
    url = f"{ELASTICSEARCH_HOST}/{index}/_search"

    all_messages = []
    search_after = None

    while True:
        payload = {
            "_source": ["message"],
            "size": 1000,
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"action.keyword": action}},
                        {"range": {"@timestamp": {"gte": from_time, "lte": to_time}}}

                    ]
                }
            },
            "sort": [
                {"@timestamp": "asc"}
            ]
        }
        if search_after:
            payload["search_after"] = search_after

        resp = requests.post(url, json=payload).json()

        hits = resp.get("hits", {}).get("hits", [])
        if not hits:
            break

        for hit in hits:
            msg_str = hit["_source"]["message"]
            try:
                msg_json = json.loads(msg_str)  # convert string to JSON
                all_messages.append(msg_json)
            except json.JSONDecodeError:
                # If message is not a JSON string, you can skip or keep as raw string
                all_messages.append({"raw_message": msg_str})
        search_after = hits[-1]["sort"]

    print(f"Total messages: {len(all_messages)}")
    # print("message: ", all_messages)
    return all_messages


if __name__ == "__main__":
    action = input("Enter action (e.g., PUSH): ").strip()
    from_time = input("Enter start timestamp (e.g., 2025-08-15T05:05:41.813Z): ").strip()
    to_time = input("Enter end timestamp (e.g., 2025-08-15T05:05:41.813Z): ").strip()
    index = input("Enter index name (default '_all'): ").strip() or "_all"

    resp = get_elk_data(action, from_time, to_time, index)
    # print(f"Total messages: {len(resp)}")
    print(resp)
