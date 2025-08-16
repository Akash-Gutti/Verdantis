import os

from huggingface_hub import HfFolder, snapshot_download

# Try to import the error type if available; otherwise fallback to Exception
try:
    from huggingface_hub.utils import HfHubHTTPError  # works on many versions
except Exception:  # pragma: no cover
    HfHubHTTPError = Exception  # fallback – we'll inspect attrs anyway

from services.common.config import settings


def _clear_tokens():
    # Remove env tokens for this process
    for k in ("HF_TOKEN", "HUGGINGFACEHUB_API_TOKEN", "HUGGING_FACE_HUB_TOKEN"):
        os.environ.pop(k, None)
    # Remove cached CLI token
    try:
        HfFolder.delete_token()
    except Exception:
        pass


def fetch(repo_id: str):
    target = f"models/{repo_id.replace('/', '__')}"
    print(f"↓ {repo_id}  →  {target}")
    try:
        snapshot_download(
            repo_id=repo_id,
            local_dir=target,
            local_dir_use_symlinks=False,
        )
    except HfHubHTTPError as e:
        # Handle both modern and older exception shapes
        status = getattr(getattr(e, "response", None), "status_code", None)
        msg = str(e)
        is_401 = status == 401 or "401" in msg or "Unauthorized" in msg
        if is_401:
            print("⚠️  401 Unauthorized. Clearing tokens and retrying anonymously…")
            _clear_tokens()
            snapshot_download(
                repo_id=repo_id,
                local_dir=target,
                local_dir_use_symlinks=False,
                token=None,  # force anonymous
            )
        else:
            raise
    except Exception as e:
        # Some older versions raise generic Exception; still catch & retry on 401-ish messages
        msg = str(e)
        if "401" in msg or "Unauthorized" in msg:
            print("⚠️  401 Unauthorized (generic). Clearing tokens and retrying anonymously…")
            _clear_tokens()
            snapshot_download(
                repo_id=repo_id,
                local_dir=target,
                local_dir_use_symlinks=False,
                token=None,
            )
        else:
            raise


if __name__ == "__main__":
    em = os.getenv("EMBEDDING_MODEL", settings.embedding_model)
    nli = os.getenv("MNLI_MODEL", settings.mnli_model)
    fetch(em)
    fetch(nli)
    print("✅ Done.")
