import json

def process_json(json_rows, extract):
    list_entries = []
    for row in json_rows:
        row_list = extract(row)
        list_entries.extend(row_list)
    list_dedup = list(set(list_entries))
    return list_dedup

def extract_director(row):
    crew_data = row['crew']
    if not crew_data or not crew_data.startswith('['):
        return []   
    try:
        crew = json.loads(crew_data)
        return [p["name"] for p in crew if p.get("job") == "Director"]
    except:
        return []
    
def extract_actor(row):
    cast_data = row['cast']
    try:
        cast = json.loads(cast_data)
        return [p["name"] for p in cast]
    except:
        return []

def extract_genre(row):
    genres_data = row['genres']
    try:
        genres = json.loads(genres_data)
        return [genre["name"] for genre in genres]
    except:
        return []

def extract_company(row):
    company_data = row['production_companies']
    try:
        companies = json.loads(company_data)
        return [company["name"] for company in companies]
    except:
        return []
