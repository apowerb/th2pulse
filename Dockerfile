# Use a slim Python image
FROM python:3.11-slim-trixie

# Release to install. The Docker workflow passes it as a build-arg, derived
# from the git tag the PyPI workflow just published. There is deliberately no
# default: a build with no --build-arg fails loudly below rather than
# silently producing an image of whatever version PyPI happens to serve.
#
# To try the working tree instead of a release, run the service directly --
# `uv run --extra ingest python -m th2pulse.ingest`. This image is for
# published versions only, so that what it reports as its version is what it
# actually contains.
ARG TH2PULSE_VERSION

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set working directory
WORKDIR /app

# Enable bytecode compilation
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV UV_NO_CACHE=1
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Dependencies first, from uv.lock, so the two published architectures and
# any later rebuild of the same tag get the same transitive versions. The
# "ingest" extra is not optional here: the CMD below runs the ingest
# service, which imports fastapi/uvicorn/asyncpg. Leaving it out is what
# made the 0.1.2 image exit 1 on start with "No module named 'fastapi'".
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --extra ingest --no-install-project

# Then the package itself, from PyPI at the tagged version. Installing the
# published wheel rather than building the working tree is what makes the
# version the image reports match the tag it is published under: the release
# workflow rewrites pyproject.toml's placeholder version in flight, for the
# wheel only, so a source build bakes in the placeholder.
RUN test -n "${TH2PULSE_VERSION}" \
    || { echo "TH2PULSE_VERSION build-arg is required, e.g. --build-arg TH2PULSE_VERSION=0.1.3" >&2; exit 1; } \
    && uv pip install --no-cache-dir "th2pulse[ingest]==${TH2PULSE_VERSION}"

# perl-base est un paquet Essential de l'image de base python:3.11-slim-trixie :
# rien au-dessus ne le tire, et cette image n'installe aucun paquet systeme.
# Il porte a lui seul les trois dernieres vulnerabilites critiques mesurees le
# 11/09 sur l'image publiee -- CVE-2026-13221, CVE-2026-42496, CVE-2026-8376 --
# sans correctif publie pour cette version de Debian. Un service Python pur ne
# l'execute jamais.
#
# Pour qui etend cette image : `apt-get install` continue de fonctionner pour
# les paquets ordinaires, et un paquet dont les scripts de maintenance sont en
# Perl le reinstallera comme n'importe quelle dependance manquante.
#
# th2etl a fait la meme purge (PR #22) et se mesure desormais a zero critique.
RUN apt-get update \
    && apt-get purge -y --allow-remove-essential perl-base \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# Meme raisonnement, meme image de base, cette fois pour util-linux et les
# paquets construits depuis la meme source (mount, login, bsdutils). Ils
# portent quatre avis HIGH -- CVE-2026-76642, -78408, -78409, -78410 -- sur des
# aides de montage qui tournent des post-hooks privilegies, sur
# `nsenter --join-cgroup` et sur la resolution de chemins X-mount. Ce service
# n'execute rien de tout cela : le CMD est du Python pur, et ni uv ni le paquet
# installe n'appellent mount, login, su ou nsenter.
#
# `apt-get autoremove` emporte ensuite libblkid1, libmount1, libsmartcols1 et
# liblastlog2-2, devenus orphelins. libuuid1 reste, et les avis continueront de
# lui etre attribues : un scanner rattache une CVE de source a tous les paquets
# binaires qui en sortent, mais le code vulnerable -- les aides de montage --
# est parti avec les binaires ci-dessus.
#
# Le coeur apowerb applique exactement cette purge (PR #137), mesuree a
# 49 avis HIGH contre 15.
RUN apt-get update \
    && apt-get purge -y --allow-remove-essential util-linux mount login bsdutils \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*


# pip et setuptools ne servent jamais a l'execution : le service tourne dans
# /opt/venv, construit par uv, qui n'embarque ni l'un ni l'autre. Les copies de
# l'image de base vendorisent leur propre arbre de dependances sous
# `setuptools/_vendor/`, et c'est CET arbre -- pas les dependances de
# l'application -- que visent les deux derniers avis Python de cette image,
# `wheel` (CVE-2026-24049) et `jaraco.context` (CVE-2026-23949). Verifie :
# aucun des deux n'a de `dist-info` dans /opt/venv.
#
# Pour qui etend cette image : `pip` n'est plus la. Utiliser uv, deja present
# dans /bin, ou lancer `python -m ensurepip` d'abord.
#
# Le coeur apowerb (apowerb/apowerb#137) et th2etl (#24) font de meme.
RUN /usr/local/bin/python -m pip uninstall -y pip setuptools \
    && rm -rf /usr/local/lib/python3.11/site-packages/pip* \
              /usr/local/lib/python3.11/site-packages/setuptools* \
              /usr/local/lib/python3.11/site-packages/pkg_resources \
              /usr/local/lib/python3.11/site-packages/_distutils_hack \
              /usr/local/lib/python3.11/site-packages/distutils-precedence.pth \
              /usr/local/lib/python3.11/site-packages/wheel*

# The library binds 127.0.0.1 by default, which is right for a process
# started on a developer machine and useless inside a container: nothing
# outside the container could reach it. Reaching past loopback is also what
# turns the tokens from optional hardening into a requirement, so the
# service now refuses to start on a reachable bind unless
# TH2PULSE_INGEST_TOKEN and TH2PULSE_QUERY_TOKEN are set (or
# TH2PULSE_ALLOW_UNAUTHENTICATED=1 says authorization lives upstream).
ENV TH2PULSE_INGEST_HOST=0.0.0.0
ENV TH2PULSE_INGEST_PORT=4319
EXPOSE 4319

# Run the application. Plain `python`, not `uv run`: `uv run` re-resolves the
# project environment on every container start, which needs network access
# from the running container and pulls the dev dependency group into a
# production image.
CMD ["python", "-m", "th2pulse.ingest"]
