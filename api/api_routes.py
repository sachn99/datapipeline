from flask import Flask, jsonify, request
from flask_restful import Api, Resource
from flask_restx import Api as SwaggerApi, Resource as SwaggerResource, fields
from sqlalchemy import create_engine, text
import pandas as pd
import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
api = Api(app)
swagger = SwaggerApi(app, version='1.0', title='Noora Health API', description='API for Noora Health WhatsApp Engagement Data')

engine = create_engine('postgresql://sachin:sachin@localhost:5432/noora')

def get_default_date_range():
    end_date = datetime.datetime.today()
    start_date = end_date - datetime.timedelta(days=30)
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

@swagger.route('/users/active-users')
class ActiveUsers(SwaggerResource):
    def get(self):
        start_date, end_date = get_default_date_range()

        query = """
        SELECT 
            DATE_TRUNC('day', inserted_at) AS day, 
            COUNT(DISTINCT masked_from_addr) AS active_users
        FROM public.dim_messages
        WHERE direction = 'inbound'
          AND inserted_at >= :start_date
          AND inserted_at < :end_date
        GROUP BY day
        ORDER BY day;
        """
        try:
            with engine.connect() as conn:
                result = conn.execute(text(query), {"start_date": start_date, "end_date": end_date}).fetchall()
                if not result:
                    return {'message': 'No active users found for the given date range'}, 404
                return jsonify([dict(row) for row in result])
        except Exception as e:
            logger.error("Error fetching active users: %s", str(e))
            return {'message': 'Internal Server Error'}, 500

@swagger.route('/users/engaged-users')
class EngagedUsers(SwaggerResource):
    def get(self):
        start_date, end_date = get_default_date_range()

        query = """
        SELECT 
            DATE_TRUNC('day', last_status_timestamp) AS day, 
            COUNT(DISTINCT masked_addressees) AS engaged_users
        FROM public.dim_messages
        WHERE last_status = 'read' 
          AND last_status_timestamp >= inserted_at 
          AND last_status_timestamp < inserted_at + INTERVAL '7 days'
          AND last_status_timestamp >= :start_date
          AND last_status_timestamp < :end_date
        GROUP BY day
        ORDER BY day;
        """
        try:
            with engine.connect() as conn:
                result = conn.execute(text(query), {"start_date": start_date, "end_date": end_date}).fetchall()
                if not result:
                    return {'message': 'No engaged users found for the given date range'}, 404
                return jsonify([dict(row) for row in result])
        except Exception as e:
            logger.error("Error fetching engaged users: %s", str(e))
            return {'message': 'Internal Server Error'}, 500

@swagger.route('/messages/status-summary')
class StatusSummary(SwaggerResource):
    def get(self):
        start_date, end_date = get_default_date_range()

        query = """
        SELECT 
            last_status, 
            COUNT(*) AS count
        FROM public.dim_messages
        WHERE inserted_at >= :start_date
          AND inserted_at < :end_date
        GROUP BY last_status
        ORDER BY last_status;
        """
        try:
            with engine.connect() as conn:
                result = conn.execute(text(query), {"start_date": start_date, "end_date": end_date}).fetchall()
                if not result:
                    return {'message': 'No messages found for the given date range'}, 404
                return jsonify([dict(row) for row in result])
        except Exception as e:
            logger.error("Error fetching status summary: %s", str(e))
            return {'message': 'Internal Server Error'}, 500

@swagger.route('/users/<string:user_id>/messages-with-status')
class UserMessagesWithStatus(SwaggerResource):
    def get(self, user_id):
        start_date, end_date = get_default_date_range()

        query = """
        SELECT 
            m.id,
            m.message_type,
            m.content,
            m.direction,
            m.external_timestamp,
            m.masked_from_addr,
            m.is_deleted,
            m.last_status,
            m.last_status_timestamp,
            m.inserted_at,
            m.updated_at
        FROM public.dim_messages m
        WHERE (masked_from_addr = :user_id OR masked_addressees = :user_id)
          AND inserted_at >= :start_date
          AND inserted_at < :end_date
        ORDER BY inserted_at;
        """
        try:
            with engine.connect() as conn:
                result = conn.execute(text(query), {"user_id": user_id, "start_date": start_date, "end_date": end_date}).fetchall()
                if not result:
                    return {'message': f'No messages found for user {user_id} in the given date range'}, 404
                return jsonify([dict(row) for row in result])
        except Exception as e:
            logger.error("Error fetching messages for user %s: %s", user_id, str(e))
            return {'message': 'Internal Server Error'}, 500

if __name__ == '__main__':
    app.run(debug=True)
