# Seattle Method — XBRL 2.1 validation

Graph: `kg19e756d44fecda0694e3`
Report: `rpt_01KSTPTS0M093B743D1HX40TC1`

## Result: ✅ **Valid XBRL 2.1**

## Summary

- **Bundle size**: 7,195 bytes
- **Files in zip**: 5 (`instance.xml, report-cal.xml, report-def.xml, report-pre.xml, report.xsd`)
- **Facts loaded by Arelle**: 46
- **Load errors**: 0
- **Validation errors**: 0

## Notes

Arelle is the de-facto XBRL processor (also used by SEC EDGAR for filing validation). Zero load errors + zero validation errors is the structural correctness claim: our output is **valid XBRL 2.1**, consumable by any standards-compliant XBRL processor, with no vendor-specific shape requirements.

This complements ``reconcile.py`` (which validates **values** — our DB facts match Charlie Hoffman's published reference at the value level across the 17 mini concepts that round-trip through our mini→rs-gaap mapping). Together they close the loop:

- **Values** match Charlie's reference (reconcile.py)
- **Shape** matches the XBRL 2.1 spec (this script)

## Errors

_None._ Arelle reported no load errors and no XBRL 2.1 validation errors against the emitted instance + schema + linkbases.
