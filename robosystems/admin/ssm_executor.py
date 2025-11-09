"""SSM executor for running commands on bastion host via AWS Systems Manager."""

import json
import subprocess
import time
from typing import Optional, Tuple

from rich.console import Console

console = Console()


class SSMExecutor:
  """Execute commands on bastion host via AWS Systems Manager."""

  def __init__(
    self,
    environment: str,
    aws_profile: str = "robosystems",
    region: str = "us-east-1",
  ):
    """Initialize SSM executor.

    Args:
        environment: Environment name (staging/prod)
        aws_profile: AWS CLI profile name
        region: AWS region
    """
    self.environment = environment
    self.aws_profile = aws_profile
    self.region = region
    self.instance_id: Optional[str] = None

  def _get_bastion_instance(self) -> str:
    """Get bastion instance ID for the environment.

    Returns:
        Instance ID

    Raises:
        RuntimeError: If instance not found
    """
    if self.instance_id:
      return self.instance_id

    cmd = [
      "aws",
      "ec2",
      "describe-instances",
      "--filters",
      f"Name=tag:Name,Values=robosystems-{self.environment}-bastion",
      "Name=instance-state-name,Values=running,stopped",
      "--query",
      "Reservations[0].Instances[0].InstanceId",
      "--output",
      "text",
      "--profile",
      self.aws_profile,
      "--region",
      self.region,
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, check=False)

    if (
      result.returncode != 0
      or not result.stdout.strip()
      or result.stdout.strip() == "None"
    ):
      raise RuntimeError(
        f"Bastion instance not found for environment: {self.environment}"
      )

    self.instance_id = result.stdout.strip()
    return self.instance_id

  def _ensure_instance_running(self, instance_id: str) -> None:
    """Ensure bastion instance is running, start if stopped.

    Args:
        instance_id: EC2 instance ID
    """
    cmd = [
      "aws",
      "ec2",
      "describe-instances",
      "--instance-ids",
      instance_id,
      "--query",
      "Reservations[0].Instances[0].State.Name",
      "--output",
      "text",
      "--profile",
      self.aws_profile,
      "--region",
      self.region,
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    state = result.stdout.strip()

    if state == "stopped":
      console.print("[yellow]🔄 Starting bastion instance...[/yellow]")

      start_cmd = [
        "aws",
        "ec2",
        "start-instances",
        "--instance-ids",
        instance_id,
        "--profile",
        self.aws_profile,
        "--region",
        self.region,
      ]
      subprocess.run(start_cmd, capture_output=True, check=True)

      wait_cmd = [
        "aws",
        "ec2",
        "wait",
        "instance-running",
        "--instance-ids",
        instance_id,
        "--profile",
        self.aws_profile,
        "--region",
        self.region,
      ]
      subprocess.run(wait_cmd, check=True)

      console.print("[green]✓ Bastion instance started[/green]")
      time.sleep(10)

  def execute(self, command: str, stream_output: bool = True) -> Tuple[str, str, int]:
    """Execute command on bastion via SSM.

    Args:
        command: Shell command to execute
        stream_output: Whether to stream output in real-time

    Returns:
        Tuple of (stdout, stderr, exit_code)

    Raises:
        RuntimeError: If command execution fails
    """
    instance_id = self._get_bastion_instance()
    self._ensure_instance_running(instance_id)

    console.print("[blue]🚀 Executing command on bastion...[/blue]")

    send_cmd = [
      "aws",
      "ssm",
      "send-command",
      "--instance-ids",
      instance_id,
      "--document-name",
      "AWS-RunShellScript",
      "--parameters",
      f'commands=["{command}"]',
      "--query",
      "Command.CommandId",
      "--output",
      "text",
      "--profile",
      self.aws_profile,
      "--region",
      self.region,
    ]

    result = subprocess.run(send_cmd, capture_output=True, text=True, check=True)
    command_id = result.stdout.strip()

    console.print(f"[dim]📋 Command ID: {command_id}[/dim]")
    console.print("[yellow]⏳ Waiting for command to complete...[/yellow]")

    wait_cmd = [
      "aws",
      "ssm",
      "wait",
      "command-executed",
      "--command-id",
      command_id,
      "--instance-id",
      instance_id,
      "--profile",
      self.aws_profile,
      "--region",
      self.region,
    ]

    subprocess.run(wait_cmd, capture_output=True, check=False)

    get_cmd = [
      "aws",
      "ssm",
      "get-command-invocation",
      "--command-id",
      command_id,
      "--instance-id",
      instance_id,
      "--profile",
      self.aws_profile,
      "--region",
      self.region,
    ]

    result = subprocess.run(get_cmd, capture_output=True, text=True, check=True)
    data = json.loads(result.stdout)

    stdout = data.get("StandardOutputContent", "")
    stderr = data.get("StandardErrorContent", "")
    exit_code = data.get("ResponseCode", -1)

    if stream_output and stdout:
      console.print("\n[bold]Output:[/bold]")
      console.print(stdout)

    if exit_code != 0:
      if stderr:
        console.print(f"\n[bold red]Error:[/bold red]\n{stderr}")
      raise RuntimeError(f"Command failed with exit code {exit_code}")

    console.print("[green]✓ Command completed successfully[/green]")
    return stdout, stderr, exit_code
