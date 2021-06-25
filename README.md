# Install redis-server
- On Linux
`sudo apt install redis-server`
- On Mac with [Homebrew](https://brew.sh/)
`brew install redis`

# Launch redis 
`redis-server`

# Create a virtual environment 
`python -m venv venv`

# Activate environment
`source venv/bin/activate`

# Install requirements
`pip install -r requirements.txt`

# Run rq workers
`python worker.py`

# Launch the API
`python api.py`
