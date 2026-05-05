import json
import pandas as pd

# 1. Load the JSON data
with open('mds.json', 'r') as f:
    data = json.load(f)

# 2. Use json_normalize to flatten nested objects automatically
# It uses dot notation by default (e.g., cast_and_crew.director)
df = pd.json_normalize(data)

# 3. Handle Arrays: Join list items with a comma as per Note #1
# MapReduce will later split this and take index [0] for the "First Genre"
for col in df.columns:
    df[col] = df[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)

# 4. Select the specific columns required by the assignment
required_cols = [
    'name', 'released_at', 'genre', 'streaming_on', 'country', 
    'type', 'content_rating', 'imdb_rating', 'number_of_seasons', 'cast_and_crew'
]

# Note: If your JSON had nested cast_and_crew, the column names 
# will now be things like 'cast_and_crew.director'
df.to_csv('mds.csv', index=False)