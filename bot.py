import os, time, threading, requests, json, re, csv, io
from flask import Flask, jsonify, Response
from collections import defaultdict

try:
    from solders.keypair import Keypair
    from solders.transaction import VersionedTransaction
    from solana.rpc.api import Client
    from solana.rpc.types import TxOpts
    _SOLANA_AVAILABLE = True
except ImportError:
    _SOLANA_AVAILABLE = False

_session = requests.Session()
_session.trust_env = False  # bypass Railway proxy env vars

app = Flask(__name__)
