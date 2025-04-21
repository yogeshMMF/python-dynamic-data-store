import os
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity
from flask_bcrypt import Bcrypt
from dotenv import load_dotenv
from sqlalchemy.exc import IntegrityError

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Configuration
app.config['SQLALCHEMY_DATABASE_URI'] = (
    f"mysql+pymysql://{os.getenv('MYSQL_USER')}:{os.getenv('MYSQL_PASSWORD')}@"
    f"{os.getenv('MYSQL_HOST')}/{os.getenv('MYSQL_DATABASE')}"
)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['JWT_SECRET_KEY'] = os.getenv('JWT_SECRET_KEY')

# Initialize extensions
db = SQLAlchemy(app)
jwt = JWTManager(app)
bcrypt = Bcrypt(app)


# Print out environment variables to debug
print("MYSQL_HOST:", os.getenv('MYSQL_HOST'))
print("MYSQL_USER:", os.getenv('MYSQL_USER'))
print("MYSQL_PASSWORD:", os.getenv('MYSQL_PASSWORD'))
print("MYSQL_DATABASE:", os.getenv('MYSQL_DATABASE'))

# User Model
class User(db.Model):
    __tablename__ = 'users'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), unique=True, nullable=False)
    password = db.Column(db.String(255), nullable=False)
    email = db.Column(db.String(255), nullable=False)
    provider_id = db.Column(db.String(255), nullable=False)
    

    def __init__(self, name, password,email,provider_id):
        self.name = name
        self.password = bcrypt.generate_password_hash(password).decode('utf-8')
        self.email = email
        self.provider_id = provider_id

# Signup Route
@app.route('/signup', methods=['POST'])
def signup():
    data = request.json
    
    # Validate input
    if not data or 'name' not in data or 'password' not in data:
        return jsonify({'message': 'Missing name or password'}), 400
    
    try:
        # Create new user
        new_user = User(
            name=data['name'], 
            password=data['password'],
            email=data['email'],
            provider_id=data['provider_id'],
        )
        
        # Add and commit to database
        db.session.add(new_user)
        db.session.commit()
        
        return jsonify({'message': 'User registered successfully'}), 200
    
    except IntegrityError:
        db.session.rollback()
        return jsonify({'message': 'Username already exists'}), 409
    
    except Exception as e:
        db.session.rollback()
        return jsonify({'message': 'Registration failed', 'error': str(e)}), 500

# Login Route
@app.route('/login', methods=['POST'])
def login():
    try:
        data = request.json
        
        # Validate input
        if not data or 'email' not in data or 'password' not in data:
            return jsonify({'message': 'Missing email or password'}), 400
        
        # Find user by email
        user = User.query.filter_by(email=data['email']).first()
        
        # Check password
        if user and bcrypt.check_password_hash(user.password, data['password']):
            # Create access token
            token = create_access_token(identity=user.id)
            return jsonify({'token': token}),200
        
        return jsonify({'message': 'Invalid credentials'}), 401
    except Exception as e:
        return jsonify({'message': 'Internal Server Error', 'error': str(e)}), 500

# Protected Route
@app.route('/protected', methods=['GET'])
@jwt_required()
def protected():
    current_user_id = get_jwt_identity()
    user = User.query.get(current_user_id)
    
    return jsonify({
        'message': 'You have access to protected route',
        'user_id': current_user_id,
        'username': user.username
    })



if __name__ == '__main__':
    app.run(debug=False)