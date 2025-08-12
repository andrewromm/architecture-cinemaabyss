from fastapi import FastAPI
from services.movies import get_movies
from services.users import get_users

app = FastAPI()


# endpoint for health check
@app.get("/health")
def health_check():
    return {"status": "healthy"}


# endpoint for list movies
@app.get("/api/movies")
def list_movies():
    return get_movies()


# endpoint for list users
@app.get("/api/users")
def list_users():
    return get_users()
