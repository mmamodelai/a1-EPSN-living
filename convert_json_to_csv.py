import json
import pandas as pd

# Load the JSON file
with open('data/activefighters.json', 'r') as f:
    data = json.load(f)

# Extract fighter names
fighter_names = data['active_fighters']

# Create DataFrame
df = pd.DataFrame({'fighters': fighter_names})

# Save to CSV
df.to_csv('data/fighters_name.csv', index=False)

print(f"Converted {len(fighter_names)} fighters to fighters_name.csv")
print(f"First 5 fighters: {fighter_names[:5]}")
print(f"Last 5 fighters: {fighter_names[-5:]}")