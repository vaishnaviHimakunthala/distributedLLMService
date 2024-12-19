import google.generativeai as genai
from dotenv import load_dotenv

import os

load_dotenv()
genai.configure(api_key=os.getenv("API_KEY"))
os.environ["GRPC_VERBOSITY"] = "ERROR"
os.environ["GLOG_minloglevel"] = "2"
def prompt_gemini(prompt):
    model = genai.GenerativeModel("gemini-1.5-flash")
    response = model.generate_content(prompt)
    return response.text.strip()
    
if __name__ == '__main__':
    print(prompt_gemini("hi"))