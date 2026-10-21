from transformer import open_weather
from transformer.core.base import Transformer

# Adding a pipeline = a new sub-package + one entry here.
TRANSFORMERS: dict[str, Transformer] = {
    t.source: t for t in (open_weather.TRANSFORMER,)
}
