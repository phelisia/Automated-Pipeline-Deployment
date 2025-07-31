from flask import Flask, request, jsonify
import os
import json
from supabase import create_client
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()
print("SUPABASE_URL:", os.getenv('SUPABASE_URL'))
print("SUPABASE_SERVICE_ROLE_KEY:", os.getenv('SUPABASE_SERVICE_ROLE_KEY'))

app = Flask(__name__)

# Connect to Supabase


# Get values from .env file
supabase = create_client(
    os.getenv('SUPABASE_URL'),
    os.getenv('SUPABASE_SERVICE_ROLE_KEY')
)

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json
    print("Incoming Data:", data)  # Keep for debugging

    # Parse the resultObject string into JSON
    if 'resultObject' in data:
        result_data = json.loads(data['resultObject'])
        print("Parsed Result Data:", result_data)

        for company in result_data:
            supabase.table('company_profile').upsert({
                'name': company['companyName'],
                'linkedin_url': company['companyUrl'],
                'followers': company['followerCount'],
                'website': company['website'],
                'description': company['description'],
                'industry': company['industry'],
                'company_size': company['companySize'],
                'specialties': [],  # No specialties in this payload
                'location': company.get('location'),
                'fetched_at': datetime.utcnow().isoformat()
            }).execute()

        return jsonify({'status': 'success', 'message': 'Company profile saved'}), 200
    else:
        return jsonify({'status': 'fail', 'message': 'No resultObject found'}), 400

    # Rest of your code...


    # Detect if this is Company Info or Posts Data
    if 'company' in data:
        # Insert/Update Company Profile
        company = data['company']
        supabase.table('company_profile').upsert({
            'name': company['name'],
            'linkedin_url': company['linkedinUrl'],
            'followers': company['followers'],
            'website': company['website'],
            'description': company['description'],
            'industry': company['industry'],
            'company_size': company['companySize'],
            'specialties': company.get('specialties', []),
            'location': company.get('location'),
            'fetched_at': datetime.utcnow().isoformat()
        }).execute()
    elif 'posts' in data:
        # Insert Posts and Engagements
        for post in data['posts']:
            post_insert = supabase.table('posts').upsert({
                'linkedin_post_id': post.get('postId'),
                'content': post.get('content'),
                'post_type': post.get('postType'),
                'published_at': post.get('publishedAt'),
                'author_id': post.get('authorId'),
                'hashtags': post.get('hashtags', []),
                'mentions': post.get('mentions', []),
                'raw_data': post
            }).execute()
            
            post_id = post_insert.data[0]['id']
            supabase.table('engagement_metrics').insert({
                'post_id': post_id,
                'likes': post.get('likes', 0),
                'comments': post.get('comments', 0),
                'shares': post.get('shares', 0),
                'impressions': post.get('impressions', 0),
                'clicks': post.get('clicks', 0),
                'engagement_rate': calculate_engagement_rate(post),
                'measured_at': datetime.utcnow().isoformat()
            }).execute()
    else:
        return jsonify({'status': 'ignored', 'message': 'No relevant data found'}), 200

    return jsonify({'status': 'success'}), 200

def calculate_engagement_rate(post):
    impressions = post.get('impressions', 1)
    total_engagement = post.get('likes', 0) + post.get('comments', 0) + post.get('shares', 0) + post.get('clicks', 0)
    return round((total_engagement / impressions) * 100, 2)

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok'}), 200



if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
app.run(host='0.0.0.0', port=port)
