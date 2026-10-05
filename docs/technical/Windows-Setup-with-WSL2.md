# Windows Setup (WSL2)

Run the RoboSystems stack on Windows through WSL2, with the repository stored inside the Linux filesystem rather than on a Windows drive. RoboSystems is developed and tested on macOS and Linux, and **WSL2 with the repository in the Linux filesystem** is the supported Windows path — it makes your environment the Linux environment we already test, rather than a third platform.

**Quick Start:** Run `wsl --install` in an admin PowerShell, enable Docker Desktop's WSL integration, clone the repo to `~/` **inside** the distro (never `/mnt/c`), install `uv`, `just`, and `jq`, then follow the normal [Local Development](Local-Development) guide.

## Why WSL2 rather than native Windows

The stack leans on POSIX conventions in a few places that are load-bearing rather than cosmetic: the `just` recipes assume a POSIX shell, several shell scripts are executed inside Linux containers, and the graph and staging engines (LadybugDB, DuckDB) do file locking against paths that must not cross a Windows/Linux filesystem boundary.

WSL2 removes all of that as a category of problem. Inside the distro you are running Linux, the container bind mounts are ext4, and every command in the wiki works verbatim. Native Windows is a best-effort configuration — see [Running natively on Windows](#running-natively-on-windows) at the end for what to expect.

## Prerequisites

- **Windows 10 version 2004 or higher (build 19041+), or Windows 11** — required by `wsl --install`.
- **Docker Desktop for Windows** with the WSL2 backend, allocated at least 8 GB of memory.
- **20 GB free disk space**, on the WSL2 virtual disk rather than `C:` directly.

## Step 1: Install WSL2

Open PowerShell **as administrator** and run:

```powershell
wsl --install
```

This enables the required Windows features and installs Ubuntu, then asks you to restart. On first launch of the distro you will be prompted to create a Linux username and password.

Confirm you are on WSL **2**, not WSL 1 — the version matters, because WSL 1 does not provide the kernel-level filesystem behavior the graph engines need:

```powershell
wsl --list --verbose
```

The `VERSION` column must read `2`. If it reads `1`, upgrade the distro:

```powershell
wsl --set-version Ubuntu 2
```

## Step 2: Allocate memory to WSL2

The stack wants 8 GB. WSL2's default ceiling is a fraction of host RAM and is frequently too low, which surfaces as OpenSearch or LadybugDB containers being killed mid-startup with no clear error.

Create `%UserProfile%\.wslconfig` (i.e. `C:\Users\<you>\.wslconfig`):

```ini
[wsl2]
memory=12GB
processors=4
```

Then apply it from PowerShell:

```powershell
wsl --shutdown
```

The next `wsl` launch picks up the new limits.

## Step 3: Connect Docker Desktop to the distro

In **Docker Desktop → Settings → Resources → WSL Integration**, enable integration for your distro (e.g. Ubuntu). This puts the `docker` and `docker compose` CLIs on the `PATH` inside WSL and points them at the Docker Desktop engine.

Verify from a WSL shell:

```bash
docker version
docker compose version
```

Both must succeed **inside** WSL. If `docker` is not found, the integration toggle did not take — re-check it and restart Docker Desktop.

## Step 4: Clone into the Linux filesystem

This is the step that matters most, and the one most commonly gotten wrong.

```bash
# Correct — the Linux filesystem, on ext4
cd ~
git clone https://github.com/RoboFinSystems/robosystems.git
cd robosystems
```

**Do not clone into `/mnt/c/...`** (i.e. anywhere under your Windows drives). Microsoft's own guidance is to [store project files in the Linux filesystem](https://learn.microsoft.com/en-us/windows/wsl/filesystems) when working from a Linux command line, and for this repo the consequences are concrete rather than merely slow:

| Symptom on `/mnt/c` | Cause |
| --- | --- |
| LadybugDB or DuckDB errors on file locks, or corrupt staging databases | Both engines keep their files in `./data` bind mounts and rely on POSIX file locking, which is unreliable across the 9p/drvfs boundary |
| Docker builds and test runs are several times slower | Every file read crosses the Windows/Linux filesystem boundary |

PostgreSQL, Valkey, OpenSearch, and LocalStack keep their data in named Docker volumes rather than under the repository, so the checkout's location does not affect them.

To browse the repo from Windows Explorer afterwards, run `explorer.exe .` from the repo directory, or enter `\\wsl$` in the Explorer address bar.

## Step 5: Install the toolchain

Inside WSL:

```bash
# uv — Python package and version management
curl -LsSf https://astral.sh/uv/install.sh | sh

# just — the task runner used throughout the wiki
curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | \
  sudo bash -s -- --to /usr/local/bin

# jq — reads your API key out of .local/config.json
sudo apt update && sudo apt install -y jq
```

Restart the shell (or `source ~/.bashrc`) so `uv` lands on the `PATH`, then confirm:

```bash
uv --version && just --version && jq --version && docker version
```

## Step 6: Follow the standard Quick Start

From here, nothing is Windows-specific. Every command in the rest of the wiki works verbatim:

```bash
just start
just demo-user
just demo-custom-graph
```

See [Local Development](Local-Development) for the full walkthrough.

## Working with the stack from Windows

- **Browsers**: WSL2 forwards `localhost` automatically, so `http://localhost:8000` (API), `:8001` (Graph API), and `:8002` (Dagster UI) open in a normal Windows browser with no extra configuration.
- **VS Code**: install the **WSL** extension and open the repo with `code .` from inside the distro. The editor runs on Windows while the language server, terminal, and Python interpreter all run in Linux — which is what you want, since the virtualenv is a Linux virtualenv.
- **Git credentials**: `git config --global credential.helper "/mnt/c/Program\\ Files/Git/mingw64/bin/git-credential-manager.exe"` reuses your Windows credential store from inside WSL, if you would rather not manage a second set.

## Troubleshooting

**`bind: An attempt was made to access a socket in a way forbidden by its access permissions`**

Windows' NAT driver reserves port ranges dynamically, and it sometimes claims ports the stack needs (8000, 8001, 8002, 5432, 6379, 4566, 9200). Check the reservations from an admin PowerShell:

```powershell
netsh interface ipv4 show excludedportrange protocol=tcp
```

If a needed port falls inside an excluded range, restart the NAT driver:

```powershell
net stop winnat
net start winnat
```

**Containers are killed during startup, or OpenSearch never becomes healthy**

WSL2 is out of memory. Raise the `memory=` value in `.wslconfig` (Step 2), then `wsl --shutdown` and start again.

**`docker: command not found` inside WSL**

Docker Desktop's WSL Integration is off for this distro. Re-enable it in Settings → Resources → WSL Integration and restart Docker Desktop.

**The stack runs but is very slow**

Confirm the repo is not on `/mnt/c`. Run `pwd` — it should print a path under `/home/`, not `/mnt/`.

## Running natively on Windows

Native Windows — PowerShell plus Docker Desktop, no WSL distro — is **not a tested configuration**, and we do not run CI against it. The notes below describe known behavior rather than a supported path.

Some groundwork is in place. The repository pins LF line endings via `.gitattributes`, so a Windows checkout no longer rewrites the container init scripts and entrypoint to CRLF; `uvloop` carries a `sys_platform != 'win32'` marker, so `uv sync` resolves; and the uvicorn event loop is selected as `auto`, so it degrades to asyncio where uvloop is absent. `just` itself works, using the `sh` provided by [Git for Windows](https://gitforwindows.org/) — it must be on your `PATH`.

Known remaining friction:

| Area | What happens |
| --- | --- |
| **Docker build mode** (`just rebuild`, `just reset-local`) | The Arelle cache bundle the build extracts (`arelle-schemas-latest.tar.gz`) is tracked as a symlink. Without Developer Mode and `git config core.symlinks true`, Git checks it out as a small text file and the build fails while extracting it. Image mode — plain `just start`, which pulls published images — is unaffected. |
| **`just test-dbt`** | Defaults its temp directory to `` `mktemp -d` ``, producing a Git Bash path that the Windows Python interpreter cannot resolve. |
| **Ports** | Subject to the same WinNAT exclusions described in Troubleshooting above. |

If you hit something not listed here, please [open an issue](https://github.com/RoboFinSystems/robosystems/issues) — but WSL2 is the configuration we can actually support.

## Related

- [Local Development](Local-Development) — the standard walkthrough, which works verbatim inside WSL2
- [Core Concepts](Core-Concepts) — what the platform does once it is running
- [Bootstrap Guide](Bootstrap-Guide) — AWS setup, if you intend to deploy your own fork
