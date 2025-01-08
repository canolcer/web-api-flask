"""
Flask API for Campaign Data and Performance Metrics

This API endpoint fetches campaign data, performance metrics, and trends from a database
and returns the data in a structured JSON format. The primary functionality includes:

1. **Campaign Card Data**: Fetches the details of campaigns, such as campaign name,
   start and end date, and the total number of days for all campaigns or a specific campaign
   based on the `campaign_id` query parameter.

2. **Performance Metrics**: Fetches the total number of impressions, clicks, and views
   for all campaigns or a specific campaign.

3. **Volume Unit Cost Trends**: Fetches the daily trends for impressions and CPM (Cost Per Thousand
   Impressions) for campaigns, with filtering based on the specified date range (`start_date` and
   `end_date`).

4. **Campaign Table**: Aggregates data from all campaigns or a specific campaign, calculating the
   averages for performance metrics (effectiveness, media, and creative scores) based on the available
   data points.

Endpoints:
- GET /campaigns: Fetches campaign data, performance metrics, and trends based on query parameters:
    - `campaign_id`: Filter by a specific campaign or 'All' to get data for all campaigns.
    - `start_date`: The start date for filtering trends (in YYYY-MM-DD format).
    - `end_date`: The end date for filtering trends (in YYYY-MM-DD format).

The response will include:
- Campaign Card with campaign details (name, range, and duration).
- Performance metrics with aggregated impressions, clicks, and views.
- Volume Unit Cost Trend with daily impressions and CPM values.
- Campaign Table with averages for effectiveness, media, and creative scores.

This API is designed to fetch and aggregate data from a relational database (e.g., PostgreSQL, MySQL)
using SQLAlchemy ORM for efficient querying.
"""

from flask import Flask, request, jsonify, send_file
from database import get_session
from datetime import datetime
from sqlalchemy import text
import json
import os

app = Flask(__name__)

# API endpoint for fetching campaign data
@app.route('/campaigns', methods=['GET'])
def get_campaign_data():
    # Get query parameters
    campaign_id = request.args.get('campaign_id', None)
    start_date_api = request.args.get('start_date', None)
    end_date_api = request.args.get('end_date', None)

    # Get database session
    session = get_session()
    
    # Initialize response data
    response_data = {}

    try:
        # If campaign_id is provided, check if it exists in the database
        if campaign_id:
            query_campaign_check = text("SELECT 1 FROM tbl_daily_campaigns WHERE campaign_id = :campaign_id LIMIT 1")
            result = session.execute(query_campaign_check, {'campaign_id': campaign_id}).scalar()

            if not result:
                # If campaign_id doesn't exist in the database, set campaign_id to 'All'
                campaign_id = 'All'

    except Exception as e:
        # In case of any database error, default to 'All' campaigns
        campaign_id = 'All'

    # From now it starts Campaign Card

    # Check if 'campaign_id' is 'All'
    if campaign_id == 'All':
        # Handle 'All' case (fetch data for all campaigns)
        
        query_all_campaigns = text("SELECT campaign_name, start_date, end_date FROM tbl_daily_scores")
        result = session.execute(query_all_campaigns)
        campaigns = result.fetchall()  # Get all campaigns data
        
        all_campaigns_data = []
        seen_campaigns = set()  # To avoid duplicate campaigns

        for campaign in campaigns:
            campaign_name, start_date_db, end_date_db = campaign
            start_date_obj, end_date_obj = datetime.strptime(start_date_db, "%Y-%m-%d"), datetime.strptime(end_date_db, "%Y-%m-%d")
            start_date_formatted, end_date_formatted = start_date_obj.strftime("%d %b"), end_date_obj.strftime("%d %b")

            # Calculate the total number of days
            total_days = (end_date_obj - start_date_obj).days
            
            # Create a unique identifier for the campaign
            campaign_key = (campaign_name, start_date_formatted, end_date_formatted)

            # Add the campaign to the response if it hasn't been added yet
            if campaign_key not in seen_campaigns:
                seen_campaigns.add(campaign_key)  # Mark this campaign as seen
                all_campaigns_data.append({
                    "campaignName": campaign_name,
                    "range": f"{start_date_formatted} - {end_date_formatted}",
                    "days": total_days,
                })
        
        # Add all campaigns data to the response_data
        response_data['campaignCard'] = all_campaigns_data
    
    elif campaign_id:
        # Handle specific campaign_id case (fetch single campaign data)
        query_campaign_name_by_id = text("SELECT campaign_name FROM tbl_daily_campaigns WHERE campaign_id = :campaign_id")
        result = session.execute(query_campaign_name_by_id, {'campaign_id': campaign_id})
        campaign_name = result.scalar()  # Get the single value (campaign_name)

        query_campaign_dates_by_id = text("SELECT start_date, end_date FROM tbl_daily_scores WHERE campaign_id = :campaign_id")
        result = session.execute(query_campaign_dates_by_id, {'campaign_id': campaign_id})
        date_range = result.fetchone()  # Get a single result (start_date, end_date)

        if campaign_name and date_range:
            # If we have both campaign name and date range
            start_date_db, end_date_db = date_range
            start_date_obj, end_date_obj = datetime.strptime(start_date_db, "%Y-%m-%d"), datetime.strptime(end_date_db, "%Y-%m-%d")
            start_date_formatted, end_date_formatted = start_date_obj.strftime("%d %b"), end_date_obj.strftime("%d %b")

            # Calculate the total number of days if start_date and end_date are provided
            total_days = (end_date_obj - start_date_obj).days

            # Format the response data for the Campaign Card
            response_data['campaignCard'] = {
                "campaignName": campaign_name,
                "range": f"{start_date_formatted} - {end_date_formatted}",
                "days": total_days,
            }
        else:
            return jsonify({'error': 'Campaign not found.'}), 404
    else:
        # Handle case where campaign_id is not provided (return all campaigns or handle differently)
        return jsonify({'error': 'No campaign_id provided.'}), 400
    
    # Performance Metrics
    if campaign_id == 'All':
        query_all_campaign_performance = text("SELECT impressions, clicks, views FROM tbl_daily_campaigns")
        result = session.execute(query_all_campaign_performance)
        interactions = result.fetchall()  # Get interaction data

        impressions = 0
        clicks = 0
        views = 0

        for interaction in interactions:
            impression, click, view = interaction

            impressions += int(impression)
            clicks += int(click)
            views += int(view)
        
        response_data["performanceMetrics"] = {
            "currentMetrics": {
                "impressions": impressions,
                "clicks": clicks,
                "views": views
            }
        }

    elif campaign_id:
        # Handle specific campaign_id case (fetch performance metrics for a specific campaign)
        query_campaign_performance_by_id = text("SELECT impressions, clicks, views FROM tbl_daily_campaigns WHERE campaign_id = :campaign_id")
        result = session.execute(query_campaign_performance_by_id, {'campaign_id': campaign_id})
        interactions = result.fetchall()  # Get the interactions data
        
        impressions = 0
        clicks = 0
        views = 0

        for interaction in interactions:
            impression, click, view = interaction

            impressions += int(impression)
            clicks += int(click)
            views += int(view)
        
        response_data["performanceMetrics"] = {
            "currentMetrics": {
                "impressions": impressions,
                "clicks": clicks,
                "views": views
            }
        }
    
    # From now it starts Volume Unit Cost Trend
    if campaign_id == 'All':
        # Set to track dates we have already processed
        control_set = set()

        # Dictionaries to store impressions and CPM values per date
        impression_dict = dict()
        cpm_dict = dict()

        # Query to get the impressions, cpm, and date from the database
        query_all_campaign_trends = text("SELECT impressions, cpm, date FROM tbl_daily_campaigns")
        result = session.execute(query_all_campaign_trends)
        trends = result.fetchall()  # Get the trends data

        for trend in trends:
            impression, cpm, input_date = trend
            
            # Parse the start and end dates from the API request, and the input date from the database
            start_date_api_obj = datetime.strptime(start_date_api, "%Y-%m-%d")
            end_date_api_obj = datetime.strptime(end_date_api, "%Y-%m-%d")
            input_date_obj = datetime.strptime(input_date, "%Y-%m-%d")

            # Check if the input date is within the requested date range
            if (input_date_obj >= start_date_api_obj) and (input_date_obj <= end_date_api_obj):
                # If the date already exists in the control set, add new impressions and CPM values
                if input_date_obj in control_set:
                    impression_dict[input_date_obj] += int(impression)
                    cpm_dict[input_date_obj] += round(float(cpm), 2)  # Round CPM to 2 decimal places
                else:
                    # If it's the first time this date appears, initialize the impressions and CPM
                    control_set.add(input_date_obj)
                    impression_dict[input_date_obj] = int(impression)
                    cpm_dict[input_date_obj] = round(float(cpm), 2)  # Round CPM to 2 decimal places

        # Sort the impressions and CPM dictionaries by date (ascending)
        sorted_impressions = sorted(impression_dict.items())
        sorted_cpm = sorted(cpm_dict.items())

        # Prepare the response data with sorted impressions and CPM values
        response_data["volumeUnitCostTrend"] = {
            "impressionsCpm": {
                "impression": {date.strftime("%Y-%m-%d"): impression for date, impression in sorted_impressions},  # Format date and store impression data
                "cpm": {date.strftime("%Y-%m-%d"): cpm for date, cpm in sorted_cpm}  # Format date and store CPM data
            }
        }

    elif campaign_id:
        # Handle specific campaign_id case (fetch trends metrics for a specific campaign)
        query_all_campaign_trends_by_id = text("SELECT impressions, cpm, date FROM tbl_daily_campaigns WHERE campaign_id = :campaign_id")
        result = session.execute(query_all_campaign_trends_by_id, {'campaign_id': campaign_id})
        trends = result.fetchall()  # Get the trends data

        # Dictionaries to store impressions and CPM values per date
        impression_dict = dict()
        cpm_dict = dict()

        for trend in trends:
            impression, cpm, input_date = trend
            
            # Parse the start and end dates from the API request, and the input date from the database
            start_date_api_obj = datetime.strptime(start_date_api, "%Y-%m-%d")
            end_date_api_obj = datetime.strptime(end_date_api, "%Y-%m-%d")
            input_date_obj = datetime.strptime(input_date, "%Y-%m-%d")

            # Check if the input date is within the requested date range
            if (input_date_obj >= start_date_api_obj) and (input_date_obj <= end_date_api_obj):

                impression_dict[input_date_obj] = int(impression)
                cpm_dict[input_date_obj] = round(float(cpm), 2)  # Round CPM to 2 decimal places

        # Sort the impressions and CPM dictionaries by date (ascending)
        sorted_impressions = sorted(impression_dict.items())
        sorted_cpm = sorted(cpm_dict.items())

        # Prepare the response data with sorted impressions and CPM values
        response_data["volumeUnitCostTrend"] = {
            "impressionsCpm": {
                "impression": {date.strftime("%Y-%m-%d"): impression for date, impression in sorted_impressions},  # Format date and store impression data
                "cpm": {date.strftime("%Y-%m-%d"): cpm for date, cpm in sorted_cpm}  # Format date and store CPM data
            }
        }

    # From now it starts Campaign Table

    # Calculate averages for each campaign's effectiveness, media, and creative scores.
    # The purpose is to obtain a close estimate of the overall performance of each campaign 
    # based on the available data points, while normalizing the different metrics.

    if campaign_id:
        # Lists to store campaigns and their corresponding data
        campaigns_data = {}  # Store all campaign data using campaign_id as the key

        # SQL query to fetch campaign data
        query_campaign_data = text("""
            SELECT start_date, end_date, campaign_id, campaign_name, effectiveness, media, creative 
            FROM tbl_daily_scores
        """)
        result = session.execute(query_campaign_data)
        campaign_data = result.fetchall()  # Fetch all campaign data

        # Iterate through all fetched campaign data
        for row in campaign_data:
            start_date, end_date, adin_id, campaign_name, effectiveness, media, creative = row
            
            # If this campaign_id is not already in campaigns_data, initialize it
            if adin_id not in campaigns_data:
                campaigns_data[adin_id] = {
                    'start_date': start_date,
                    'end_date': end_date,
                    'campaign_name': campaign_name,
                    'total_effectiveness': 0,
                    'effectiveness_count': 0,
                    'total_media_score': 0,
                    'media_count': 0,
                    'total_creative_score': 0,
                    'creative_count': 0
                }
            
            # Update the campaign's scores
            campaigns_data[adin_id]['total_effectiveness'] += effectiveness
            campaigns_data[adin_id]['effectiveness_count'] += 1
            
            campaigns_data[adin_id]['total_media_score'] += media
            campaigns_data[adin_id]['media_count'] += 1
            
            campaigns_data[adin_id]['total_creative_score'] += creative
            campaigns_data[adin_id]['creative_count'] += 1

        # After processing all campaigns, calculate the averages for each campaign
        for adin_id, data in campaigns_data.items():
            if data['effectiveness_count'] > 0:
                data['average_effectiveness'] = int(data['total_effectiveness'] / data['effectiveness_count'])
            else:
                data['average_effectiveness'] = 0

            if data['media_count'] > 0:
                data['average_media_score'] = int(data['total_media_score'] / data['media_count'])
            else:
                data['average_media_score'] = 0

            if data['creative_count'] > 0:
                data['average_creative_score'] = int(data['total_creative_score'] / data['creative_count'])
            else:
                data['average_creative_score'] = 0
        
        response_data["campaignTable"] = {
            "start_date": [],
            "end_date": [],
            "adin_id": [],
            "campaign": [],
            "effectiveness": [],
            "media": [],
            "creative": []
        }

        for keys in campaigns_data.keys():
            response_data["campaignTable"]["start_date"].append(campaigns_data[keys]['start_date'])
            response_data["campaignTable"]["end_date"].append(campaigns_data[keys]['end_date'])
            response_data["campaignTable"]["adin_id"].append(keys)
            response_data["campaignTable"]["campaign"].append(campaigns_data[keys]['campaign_name'])
            response_data["campaignTable"]["effectiveness"].append(campaigns_data[keys]['average_effectiveness'])
            response_data["campaignTable"]["media"].append(campaigns_data[keys]['average_media_score'])
            response_data["campaignTable"]["creative"].append(campaigns_data[keys]['average_creative_score'])


    file_path = 'response.json'
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    # Write the response data to a JSON file
    with open(file_path, 'w') as json_file:
        json.dump(response_data, json_file, indent=4)  # Writing to response.json with pretty formatting

    # Return the data in the requested JSON format
    return jsonify(response_data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
