"""
AI Client - AWS Bedrock interface for Claude models.

Production-grade AI client using AWS Bedrock exclusively for:
- Cost tracking in AWS Cost Explorer
- CloudWatch metrics and monitoring
- IAM-based access control
"""

import json
from typing import List, Optional
from dataclasses import dataclass

from robosystems.config import env, AgentConfig, BedrockModel
from robosystems.logger import logger


@dataclass
class AIMessage:
  role: str
  content: str


@dataclass
class AIResponse:
  content: str
  model: str
  input_tokens: int
  output_tokens: int
  stop_reason: Optional[str] = None


class AIClient:
  """
  AWS Bedrock AI client for Claude models.

  Uses AWS Bedrock exclusively for all AI operations to ensure:
  - All costs appear in AWS Cost Explorer
  - CloudWatch metrics for token usage
  - IAM-based access control
  """

  def __init__(self):
    self.backend = "bedrock"
    self.client = self._initialize_bedrock_client()
    logger.info("Initialized AI client with AWS Bedrock")

  def _initialize_bedrock_client(self):
    """Initialize AWS Bedrock client."""
    import boto3

    # Build real AWS endpoint URL (bypass LocalStack's AWS_ENDPOINT_URL env var)
    bedrock_endpoint = f"https://bedrock-runtime.{env.AWS_BEDROCK_REGION}.amazonaws.com"

    kwargs = {
      "service_name": "bedrock-runtime",
      "region_name": env.AWS_BEDROCK_REGION,
      "endpoint_url": bedrock_endpoint,  # IMPORTANT: Bypass LocalStack, go directly to AWS
    }

    # In dev: use explicit credentials (AWS_BEDROCK_ACCESS_KEY_ID)
    # In prod/staging: use IAM role credentials (ECS task role / EC2 instance profile)
    if env.ENVIRONMENT == "dev" and env.AWS_BEDROCK_ACCESS_KEY_ID:
      kwargs["aws_access_key_id"] = env.AWS_BEDROCK_ACCESS_KEY_ID
      kwargs["aws_secret_access_key"] = env.AWS_BEDROCK_SECRET_ACCESS_KEY
      logger.info("Using Bedrock with dev credentials (AWS_BEDROCK_ACCESS_KEY_ID)")
    else:
      logger.info(
        f"Using Bedrock with IAM role credentials (environment: {env.ENVIRONMENT})"
      )

    try:
      client = boto3.client(**kwargs)
      # Verify credentials work (skip in dev - LocalStack doesn't have STS)
      if env.ENVIRONMENT != "dev":
        sts_kwargs = {"service_name": "sts", "region_name": env.AWS_BEDROCK_REGION}
        if env.AWS_BEDROCK_ACCESS_KEY_ID:
          sts_kwargs["aws_access_key_id"] = env.AWS_BEDROCK_ACCESS_KEY_ID
          sts_kwargs["aws_secret_access_key"] = env.AWS_BEDROCK_SECRET_ACCESS_KEY
        boto3.client(**sts_kwargs).get_caller_identity()
      return client
    except Exception as e:
      raise ValueError(
        f"Failed to initialize AWS Bedrock client: {e}\n"
        "Ensure AWS credentials are configured (aws configure) or set:\n"
        "  AWS_BEDROCK_ACCESS_KEY_ID and AWS_BEDROCK_SECRET_ACCESS_KEY"
      )

  def _get_model_id(
    self, model: Optional[str] = None, agent_type: Optional[str] = None
  ) -> str:
    """
    Get the Bedrock model ID.

    Args:
        model: Optional model name override (e.g., 'claude-3-5-sonnet-20241022')
        agent_type: Optional agent type to check for overrides

    Returns:
        Bedrock model ID string
    """
    if model:
      try:
        model_enum = BedrockModel(model)
      except ValueError:
        logger.warning(f"Invalid model '{model}', using default")
        model_enum = None
    else:
      model_enum = None

    bedrock_id = AgentConfig.get_bedrock_model_id(
      model=model_enum, agent_type=agent_type
    )
    logger.debug(f"Using Bedrock model: {bedrock_id}")
    return bedrock_id

  async def create_message(
    self,
    messages: List[AIMessage],
    system: Optional[str] = None,
    max_tokens: int = 4000,
    temperature: float = 0.7,
    model: Optional[str] = None,
    agent_type: Optional[str] = None,
  ) -> AIResponse:
    """
    Create a message using AWS Bedrock.

    Args:
        messages: List of conversation messages
        system: Optional system prompt
        max_tokens: Maximum tokens in response
        temperature: Sampling temperature (0-1)
        model: Optional model name override
        agent_type: Optional agent type for model override lookup

    Returns:
        AIResponse with content and token usage
    """
    model_id = self._get_model_id(model, agent_type)
    return await self._bedrock_create_message(
      messages, system, max_tokens, temperature, model_id
    )

  async def _bedrock_create_message(
    self,
    messages: List[AIMessage],
    system: Optional[str],
    max_tokens: int,
    temperature: float,
    model: str,
  ) -> AIResponse:
    """Create message using AWS Bedrock."""
    message_dicts = [{"role": msg.role, "content": msg.content} for msg in messages]

    request_body = {
      "anthropic_version": "bedrock-2023-05-31",
      "max_tokens": max_tokens,
      "temperature": temperature,
      "messages": message_dicts,
    }

    if system:
      request_body["system"] = system

    response = self.client.invoke_model(
      modelId=model,
      body=json.dumps(request_body),
    )

    response_body = json.loads(response["body"].read())

    return AIResponse(
      content=response_body["content"][0]["text"],
      model=model,
      input_tokens=response_body["usage"]["input_tokens"],
      output_tokens=response_body["usage"]["output_tokens"],
      stop_reason=response_body.get("stop_reason"),
    )
