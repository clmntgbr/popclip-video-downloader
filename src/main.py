import os
import yt_dlp
import uuid

from kombu import Queue
from flask import Flask
from celery import Celery

from src.config import Config
from src.s3_client import S3Client
from src.rabbitmq_client import RabbitMQClient
from src.file_client import FileClient
from src.converter import ProtobufConverter
from src.Protobuf.Message_pb2 import Clip, Video, ClipStatus, VideoDownloaderMessage

app = Flask(__name__)
app.config.from_object(Config)
s3_client = S3Client(Config)
rmq_client = RabbitMQClient()
file_client = FileClient()

celery = Celery("tasks", broker=app.config["RABBITMQ_URL"])

celery.conf.update(
    {
        "task_serializer": "json",
        "accept_content": ["json"],
        "broker_connection_retry_on_startup": True,
        "task_routes": {
            "tasks.process_message": {"queue": app.config["RMQ_QUEUE_WRITE"]}
        },
        "task_queues": [
            Queue(
                app.config["RMQ_QUEUE_READ"], routing_key=app.config["RMQ_QUEUE_READ"]
            )
        ],
    }
)


@celery.task(name="tasks.process_message", queue=app.config["RMQ_QUEUE_READ"])
def process_message(message):
    try:
        clip: Clip = ProtobufConverter.json_to_protobuf(message)
        format = "bestvideo[height<=720]+bestaudio/best[height<=720]"

        tmpVideoPath = f"/tmp/{clip.id}.mp4"

        info = get_video_info(format, clip.url)
        download_video(format, clip.url, tmpVideoPath)

        video = create_original_video(info, get_file_size(tmpVideoPath))

        keyVideo = f"{clip.userId}/{clip.id}/{video.name}"

        if not s3_client.upload_file(tmpVideoPath, keyVideo):
            raise Exception()

        file_client.delete_file(tmpVideoPath)

        clip.originalVideo.CopyFrom(video)

        clip.status = ClipStatus.Name(ClipStatus.VIDEO_DOWNLOADER_COMPLETE)

        protobuf = VideoDownloaderMessage()
        protobuf.clip.CopyFrom(clip)

        if not rmq_client.send_message(
            protobuf, "App\\Protobuf\\VideoDownloaderMessage"
        ):
            raise Exception()

        return True

    except Exception:
        clip.status = ClipStatus.Name(ClipStatus.VIDEO_DOWNLOADER_ERROR)

        protobuf = VideoDownloaderMessage()
        protobuf.clip.CopyFrom(clip)

        if not rmq_client.send_message(
            protobuf, "App\\Protobuf\\VideoDownloaderMessage"
        ):
            return False


def get_video_info(format, url):
    ydl_opts = {
        "quiet": True,
        "skip_download": True,
        "format": format,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)
        return info.get("duration")


def download_video(format, url, output_path):
    ydl_opts = {
        "format": format,
        "outtmpl": output_path,
        "merge_output_format": "mp4",
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])


def get_file_size(path):
    size_bytes = os.path.getsize(path)
    return size_bytes


def create_original_video(length, size) -> Video:
    id = str(uuid.uuid4())
    video = Video()
    video.id = id
    video.name = f"{id}.mp4"
    video.mimeType = "video/mp4"
    video.originalName = video.name

    video.size = size
    video.length = length

    video.IsInitialized()

    return video
