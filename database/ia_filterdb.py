import logging
import re
import base64
from struct import pack
from pyrogram.file_id import FileId
from pymongo.errors import DuplicateKeyError
from umongo import Document, fields
from motor.motor_asyncio import AsyncIOMotorClient
from umongo.frameworks.motor_asyncio import MotorAsyncIOInstance
from marshmallow.exceptions import ValidationError
from info import DATABASE_URI, DATABASE_NAME, COLLECTION_NAME, USE_CAPTION_FILTER, MAX_BTN

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Connect to MongoDB
client = AsyncIOMotorClient(DATABASE_URI)
db = client[DATABASE_NAME]

# For umongo >= 3.x
instance = MotorAsyncIOInstance(db)

# ----------------------------------------------------
# Media Document Schema
# ------------------------------------------------------
@instance.register
class Media(Document):
    _id = fields.StrField(required=True)  # MongoDB's primary key
    file_ref = fields.StrField(allow_none=True)
    file_name = fields.StrField(required=True)
    file_size = fields.IntField(required=True)
    file_type = fields.StrField(allow_none=True)
    mime_type = fields.StrField(allow_none=True)
    caption = fields.StrField(allow_none=True)

    class Meta:
        collection_name = COLLECTION_NAME
        indexes = ('$file_name',)


# ------------------------------------------------------
# Database utility functions
# ------------------------------------------------------
async def save_file(media):
    """Save file in database"""
    file_id, file_ref = unpack_new_file_id(media.file_id)
    file_name = re.sub(r"(_|\-|\.|\+)", " ", str(media.file_name))

    try:
        file = Media(
            _id=file_id,
            file_ref=file_ref,
            file_name=file_name,
            file_size=media.file_size,
            file_type=media.file_type,
            mime_type=media.mime_type,
            caption=media.caption.html if media.caption else None,
        )
    except ValidationError:
        logger.exception("Error occurred while saving file in database")
        return False, 2

    try:
        await file.commit()
    except DuplicateKeyError:
        logger.warning(f"{getattr(media, 'file_name', 'NO_FILE')} already exists in database")
        return False, 0
    else:
        logger.info(f"{getattr(media, 'file_name', 'NO_FILE')} saved to database")
        return True, 1


async def get_search_results(query, file_type=None, max_results=MAX_BTN, offset=0):
    """For given query return (results, next_offset, total_results)"""
    query = query.strip()

    if not query:
        raw_pattern = "."
    elif " " not in query:
        raw_pattern = r"(\b|[\.\+\-_])" + query + r"(\b|[\.\+\-_])"
    else:
        raw_pattern = query.replace(" ", r".*[\s\.\+\-_]")

    try:
        regex = re.compile(raw_pattern, flags=re.IGNORECASE)
    except re.error:
        return [], "", 0

    if USE_CAPTION_FILTER:
        filter_q = {"$or": [{"file_name": regex}, {"caption": regex}]}
    else:
        filter_q = {"file_name": regex}

    if file_type:
        filter_q["file_type"] = file_type

    # use db[collection] directly for async motor ops
    total_results = await db[COLLECTION_NAME].count_documents(filter_q)
    next_offset = offset + max_results
    if next_offset > total_results:
        next_offset = ""

    cursor = db[COLLECTION_NAME].find(filter_q).sort("$natural", -1).skip(offset).limit(max_results)
    files = await cursor.to_list(length=max_results)

    return files, next_offset, total_results


async def get_file_details(query):
    filter_q = {"_id": query}
    cursor = db[COLLECTION_NAME].find(filter_q)
    filedetails = await cursor.to_list(length=1)
    return filedetails


# ------------------------------------------------------
# Helpers for encoding/decoding
# ------------------------------------------------------
def encode_file_id(s: bytes) -> str:
    r = b""
    n = 0
    for i in s + bytes([22]) + bytes([4]):
        if i == 0:
            n += 1
        else:
            if n:
                r += b"\x00" + bytes([n])
                n = 0
            r += bytes([i])
    return base64.urlsafe_b64encode(r).decode().rstrip("=")


def encode_file_ref(file_ref: bytes) -> str:
    return base64.urlsafe_b64encode(file_ref).decode().rstrip("=")


def unpack_new_file_id(new_file_id):
    """Return file_id, file_ref"""
    decoded = FileId.decode(new_file_id)
    file_id = encode_file_id(
        pack(
            "<iiqq",
            int(decoded.file_type),
            decoded.dc_id,
            decoded.media_id,
            decoded.access_hash,
        )
    )
    file_ref = encode_file_ref(decoded.file_reference)
    return file_id, file_ref
