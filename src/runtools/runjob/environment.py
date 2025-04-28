from typing import List, Annotated, Union

from pydantic import BaseModel, Field, Discriminator

from runtools.runcore.db import PersistenceConfig
from runtools.runcore.env import LocalEnvironmentConfig, EnvironmentConfig


class PluginsConfig(BaseModel):
    """Configuration for runtools plugins system."""
    enabled: bool = Field(
        default=False,
        description="Whether the plugins system is enabled"
    )
    load: List[str] = Field(
        default_factory=list,
        description="List of plugin names to load when the environment starts"
    )


class LocalRunEnvironmentConfig(LocalEnvironmentConfig):
    """Extended configuration for local environments that run jobs."""
    plugins: PluginsConfig = Field(
        default_factory=PluginsConfig,
        description="Configuration for plugins in the job execution environment"
    )


class IsolatedEnvironmentConfig(EnvironmentConfig):
    """Configuration for isolated environments used primarily in testing."""
    type: str = "isolated"
    persistence: PersistenceConfig = Field(
        default_factory=PersistenceConfig.in_memory_sqlite,
        description="Persistence configuration, defaults to in-memory SQLite for testing"
    )
    plugins: PluginsConfig = Field(
        default_factory=PluginsConfig,
        description="Configuration for plugins in the isolated environment"
    )


RunEnvironmentConfigUnion = Annotated[
    Union[LocalRunEnvironmentConfig, IsolatedEnvironmentConfig],
    Discriminator("type")
]