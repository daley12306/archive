
import os
os.environ["HF_HOME"] = "/opt/models/hf_cache"

from transformers import AutoTokenizer, AutoModelForTokenClassification

MODEL_NAME = "zikay3624/careerlake-ner-skill"
LOCAL_PATH = "/opt/models/hf_cache/careerlake-ner-skill"

print(f"Downloading {MODEL_NAME} ...")
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForTokenClassification.from_pretrained(MODEL_NAME)

tokenizer.save_pretrained(LOCAL_PATH)
model.save_pretrained(LOCAL_PATH)
print(f"Saved to {LOCAL_PATH}")