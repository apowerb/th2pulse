<p align="center">
  <img src="https://docs.apowerb.com/logo/apowerb-wide.png" alt="apowerb" height="80"/>
</p>

<p align="center">
  <strong>OpenTelemetry collection and observability for the apowerb stack.</strong>
</p>

<p align="center">
  <a href="https://docs.apowerb.com/">Documentation</a> •
  <a href="https://github.com/apowerb/th2pulse">GitHub</a> •
  <a href="https://thaink2.com">thaink2</a>
</p>

---

## What is this image?

A lightweight OpenTelemetry collection and observability library for the
[**apowerb**](https://github.com/apowerb/apowerb) stack — used by apowerb, th2llm
and th2etl.

Google ADK wires its own OpenTelemetry providers from the environment; th2pulse gives
you one consistent way to collect traces, metrics and logs across the services.

## Usage

```python
import th2pulse

th2pulse.init_observability("apowerb")
```

See the [supervision guide](https://docs.apowerb.com/monitoring/supervision).

## Tags

| Tag | Content |
|-----|---------|
| `latest` | Latest published release |
| `x.y.z` | A specific release |

## License

Apache-2.0. Source and issues on [GitHub](https://github.com/apowerb/th2pulse).
