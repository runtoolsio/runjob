from typing import List, Annotated, Union

from pydantic import BaseModel, Field, Discriminator

from runtools.runcore.env import LocalEnvironmentConfig


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


RunEnvironmentConfigUnion = Annotated[
    Union[LocalRunEnvironmentConfig],
    Discriminator("type")
]
