from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.responses import PlainTextResponse, Response
from pydantic import BaseModel
import httpx
from typing import Dict, List, Optional
from datetime import datetime
from pubsub import pub
import uuid
import logging

# ロギングの設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Subscription API", version="1.0.0")

# データモデル
class Subscription(BaseModel):
    subscription_id: str
    topic: str
    callback_url: str
    created_at: datetime
    status: str = "active"
    filters: Optional[Dict] = None

class SubscriptionRequest(BaseModel):
    topic: str
    callback_url: str
    filters: Optional[Dict] = None

# インメモリストレージ（実際の実装では永続化が必要）
subscriptions: Dict[str, Subscription] = {}

# トピックのバリデーション
VALID_TOPICS = ["orders", "users", "products"]  # 例として

def validate_topic(topic: str) -> bool:
    if topic not in VALID_TOPICS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid topic. Must be one of: {VALID_TOPICS}"
        )
    return True

# API エンドポイント
@app.post("/subscriptions", response_model=Subscription)
async def create_subscription(request: SubscriptionRequest):
    # トピックの検証
    validate_topic(request.topic)
    
    # 新規サブスクリプションの作成
    subscription_id = str(uuid.uuid4())
    subscription = Subscription(
        subscription_id=subscription_id,
        topic=request.topic,
        callback_url=request.callback_url,
        created_at=datetime.utcnow(),
        filters=request.filters
    )
    
    # ストレージに保存
    subscriptions[subscription_id] = subscription
    
    # PubSubへの登録
    try:
        pub.subscribe(listener=lambda msg: handle_message(msg, subscription),
                     topicName=request.topic)
        logger.info(f"Successfully subscribed to topic: {request.topic}")
    except Exception as e:
        logger.error(f"Failed to subscribe to PubSub: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to create subscription")
    
    return subscription

@app.get("/subscriptions/{subscription_id}", response_model=Subscription)
async def get_subscription(subscription_id: str):
    if subscription_id not in subscriptions:
        raise HTTPException(status_code=404, detail="Subscription not found")
    return subscriptions[subscription_id]

@app.get("/subscriptions", response_model=List[Subscription])
async def list_subscriptions():
    return list(subscriptions.values())

@app.post("/v1/api/sendQuery")
async def send_query(request: Request):
    try:
        # フォームデータからクエリを取得
        form_data = await request.form()
        query = form_data.get("query")
        if not query:
            raise HTTPException(status_code=400, detail="Query parameter is required")

        # Accept ヘッダーの取得
        accept_header = request.headers.get("Accept", "application/json")

        # SPARQLエンドポイントへのリクエストを構築
        sparql_endpoint = "http://localhost:8890"
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                sparql_endpoint,
                data={"query": query},
                headers={"Accept": accept_header}
            )

            # レスポンスのステータスコードとContent-Typeを保持
            content_type = response.headers.get("Content-Type", "text/plain")
            
            # エラーハンドリング
            if response.status_code != 200:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"SPARQL endpoint error: {response.text}"
                )

            # レスポンスを返却（Content-Typeを維持）
            return Response(
                content=response.content,
                media_type=content_type,
                status_code=response.status_code
            )

    except httpx.RequestError as e:
        logger.error(f"Error forwarding query: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to forward query: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )

@app.delete("/subscriptions/{subscription_id}")
async def delete_subscription(subscription_id: str):
    if subscription_id not in subscriptions:
        raise HTTPException(status_code=404, detail="Subscription not found")
    
    subscription = subscriptions[subscription_id]
    
    # PubSubからの登録解除
    try:
        pub.unsubscribe(listener=lambda msg: handle_message(msg, subscription),
                       topicName=subscription.topic)
        logger.info(f"Successfully unsubscribed from topic: {subscription.topic}")
    except Exception as e:
        logger.error(f"Failed to unsubscribe from PubSub: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to delete subscription")
    
    del subscriptions[subscription_id]
    return {"status": "success", "message": "Subscription deleted"}

# メッセージハンドリング
async def handle_message(msg, subscription: Subscription):
    """
    受信したメッセージを処理し、コールバックURLに転送する
    """
    try:
        # フィルタリングの適用
        if subscription.filters and not apply_filters(msg, subscription.filters):
            return
        
        # ここでコールバックURLにメッセージを転送する実装を追加
        # 例: httpxを使用したPOSTリクエスト
        logger.info(f"Message received for subscription {subscription.subscription_id}")
        
    except Exception as e:
        logger.error(f"Error handling message: {str(e)}")

def apply_filters(msg: dict, filters: Dict) -> bool:
    """
    メッセージにフィルターを適用する
    """
    # フィルタリングロジックの実装
    # 例: 特定のフィールドの値に基づくフィルタリング
    return True