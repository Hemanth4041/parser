"""
KMS Encryption module using organisation_biz_id for key lookup
"""
import base64
import logging
from typing import Dict, List, Optional
from google.cloud import kms
from common.env_variables.settings import PROJECT_ID
from common.env_variables.settings import KEY_RING
from common.env_variables.settings import LOCATION

logger = logging.getLogger(__name__)


class KmsEncryptor:
    """Handles encryption using KMS with key caching for performance."""
    
    def __init__(self):
        self.kms_client = kms.KeyManagementServiceClient()
        self._key_cache: Dict[str, str] = {}  # {organisation_biz_id: key_name}
    
    def _find_and_cache_key(self, organisation_biz_id: str) -> str:
        """Finds and caches the CMEK key for an organisation to reduce API calls."""
        if organisation_biz_id in self._key_cache:
            return self._key_cache[organisation_biz_id]
        
        logger.info(f"Looking up KMS key for organisation_biz_id: {organisation_biz_id}")
        parent = f"projects/{PROJECT_ID}/locations/{LOCATION}/keyRings/{KEY_RING}"
        
        try:
            keys = self.kms_client.list_crypto_keys(request={"parent": parent})
            for key in keys:
                if key.labels and key.labels.get("organisation_biz_id") == organisation_biz_id:
                    self._key_cache[organisation_biz_id] = key.name
                    logger.info(f"Found and cached key '{key.name}' for organisation '{organisation_biz_id}'")
                    return key.name
        except Exception as e:
            logger.error(f"Failed to list KMS keys in '{parent}': {e}", exc_info=True)
            raise
        
        raise ValueError(f"No CMEK found with label organisation_biz_id={organisation_biz_id} in key ring {KEY_RING}")
    
    def _encrypt_value(self, organisation_biz_id: str, plaintext: str) -> Optional[str]:
        """Encrypts a single plaintext value for an organisation using KMS."""
        # Skip encryption for None or empty strings
        if plaintext is None or (isinstance(plaintext, str) and plaintext.strip() == ""):
            return plaintext
        
        key_name = self._find_and_cache_key(organisation_biz_id)
        response = self.kms_client.encrypt(
            request={"name": key_name, "plaintext": str(plaintext).encode("utf-8")}
        )
        return base64.b64encode(response.ciphertext).decode("utf-8")
    
    def encrypt_row(self, row: Dict, sensitive_fields: List[str]) -> Dict:
        """Encrypts all sensitive fields within a row using organisation_biz_id."""
        organisation_biz_id = row.get("organisation_biz_id")
        if not organisation_biz_id:
            raise ValueError("Row is missing 'organisation_biz_id' for encryption key lookup.")
        
        encrypted_row = row.copy()
        for field in sensitive_fields:
            if field in row:
                try:
                    # _encrypt_value now handles None and empty string checks
                    encrypted_row[field] = self._encrypt_value(organisation_biz_id, row[field])
                except Exception as e:
                    logger.error(f"Encryption failed for field '{field}' for organisation '{organisation_biz_id}'", exc_info=True)
                    raise
        
        # organisation_biz_id remains in the row (it's a required field in the schema)
        return encrypted_row