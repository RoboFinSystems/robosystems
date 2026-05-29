# The World Online — XBRL 2.1 Validation (Arelle)

## Result: ✅ **Valid XBRL 2.1**

- **Package**: `world-online.zip` (7,448 bytes)
- **Files in zip**: 5 (`instance.xml, report-cal.xml, report-def.xml, report-pre.xml, report.xsd`)
- **Facts loaded by Arelle**: 55
- **Load errors**: 0
- **Validation errors**: 0

Validated on the host with **Arelle** (the de-facto XBRL processor, also used by SEC EDGAR) directly against the on-disk report package — no API, no container. Zero load + validation errors is the structural-correctness claim: the output is valid XBRL 2.1, consumable by any standards-compliant processor. This is **base XBRL 2.1** validation; SEC/EFM disclosure-system checks are not enabled (the instance isn't an SEC filing).

## Errors

_None._ Arelle reported no load errors and no XBRL 2.1 validation errors against the emitted instance + schema + linkbases.
