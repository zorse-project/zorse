"""License classification utilities."""

from typing import Optional

# Set of permissive license SPDX IDs from The Stack v2 license_stats.csv
# Based on Blue Oak Council and ScanCode permissive/public domain licenses
PERMISSIVE_LICENSE_SPDX_IDS = {
    "0BSD", "AAL", "AdaCore-doc", "Adobe-2006", "Adobe-Glyph", "ADSL",
    "AFL-1.1", "AFL-1.2", "AFL-2.0", "AFL-2.1", "AFL-3.0", "AML", "AMPAS",
    "ANTLR-PD", "Apache-1.0", "Apache-1.1", "Apache-2.0", "APAFML",
    "Artistic-1.0", "Artistic-1.0-cl8", "Artistic-1.0-Perl", "Artistic-2.0",
    "Barr", "Beerware", "Bitstream-Charter", "Bitstream-Vera", "blessing",
    "BlueOak-1.0.0", "Boehm-GC", "Borceux", "BSD-1-Clause", "BSD-2-Clause",
    "BSD-2-Clause-Patent", "BSD-2-Clause-Views", "BSD-3-Clause",
    "BSD-3-Clause-Attribution", "BSD-3-Clause-Clear", "BSD-3-Clause-LBNL",
    "BSD-3-Clause-Modification", "BSD-3-Clause-No-Nuclear-License-2014",
    "BSD-3-Clause-No-Nuclear-Warranty", "BSD-3-Clause-Open-MPI",
    "BSD-4-Clause", "BSD-4-Clause-Shortened", "BSD-4-Clause-UC", "BSD-4.3RENO",
    "BSD-4.3TAHOE", "BSD-Advertising-Acknowledgement",
    "BSD-Attribution-HPND-disclaimer", "BSD-Source-Code", "BSL-1.0",
    "bzip2-1.0.6", "Caldera", "CC-BY-1.0", "CC-BY-2.0", "CC-BY-2.5",
    "CC-BY-2.5-AU", "CC-BY-3.0", "CC-BY-3.0-AT", "CC-BY-3.0-DE",
    "CC-BY-3.0-NL", "CC-BY-3.0-US", "CC-BY-4.0", "CC-PDDC", "CC0-1.0",
    "CDLA-Permissive-1.0", "CDLA-Permissive-2.0", "CECILL-B", "CERN-OHL-1.1",
    "CERN-OHL-1.2", "CERN-OHL-P-2.0", "CFITSIO", "ClArtistic", "Clips",
    "CMU-Mach", "CNRI-Python", "COIL-1.0", "Community-Spec-1.0", "Condor-1.1",
    "Crossword", "Cube", "curl", "diffmark", "DOC", "DSDP", "dtoa", "ECL-1.0",
    "ECL-2.0", "EFL-1.0", "EFL-2.0", "eGenix", "Entessa", "EPICS",
    "etalab-2.0", "EUDatagrid", "Fair", "FreeBSD-DOC", "FSFAP", "FSFUL",
    "FSFULLR", "FSFULLRWD", "FTL", "GD", "Giftware", "Glulxe", "GLWTPL",
    "Graphics-Gems", "HaskellReport", "HP-1986", "HPND",
    "HPND-Markus-Kuhn", "HPND-sell-variant", "HPND-sell-variant-MIT-disclaimer",
    "HTMLTIDY", "ICU", "IJG", "IJG-short", "ImageMagick", "iMatix",
    "Info-ZIP", "Intel", "Intel-ACPI", "ISC", "Jam", "JasPer-2.0", "JPNIC",
    "JSON", "Kazlib", "Latex2e", "Latex2e-translated-notice", "Leptonica",
    "Libpng", "libpng-2.0", "libselinux-1.0", "libtiff", "Linux-OpenIB",
    "LLVM-exception", "LOOP", "LPL-1.02", "LPPL-1.3c", "LZMA-SDK-9.22",
    "Martin-Birgmeier", "metamail", "Minpack", "MirOS", "MIT", "MIT-0",
    "MIT-advertising", "MIT-CMU", "MIT-enna", "MIT-feh", "MIT-Festival",
    "MIT-Modern-Variant", "MIT-open-group", "MIT-Wu", "MITNFA", "mpich2",
    "mplus", "MS-LPL", "MS-PL", "MTLL", "MulanPSL-1.0", "MulanPSL-2.0",
    "Multics", "Mup", "NAIST-2003", "NASA-1.3", "NCSA", "Net-SNMP", "NetCDF",
    "Newsletr", "NICTA-1.0", "NIST-PD", "NIST-PD-fallback", "NIST-Software",
    "NLOD-1.0", "NLOD-2.0", "NLPL", "NRL", "NTP", "NTP-0", "O-UDA-1.0",
    "ODC-By-1.0", "OFFIS", "OFL-1.0", "OFL-1.0-RFN", "OGC-1.0",
    "OGL-Canada-2.0", "OGL-UK-1.0", "OGL-UK-2.0", "OGL-UK-3.0", "OGTSL",
    "OLDAP-2.0.1", "OLDAP-2.4", "OLDAP-2.5", "OLDAP-2.7", "OLDAP-2.8",
    "OML", "OpenSSL", "OPUBL-1.0", "PDDL-1.0", "PHP-3.0", "PHP-3.01",
    "Plexus", "PostgreSQL", "PSF-2.0", "psutils", "Python-2.0", "Qhull",
    "RSA-MD", "Ruby", "SAX-PD", "Saxpath", "SCEA", "SchemeReport", "Sendmail",
    "SGI-B-1.1", "SGI-B-2.0", "SHL-0.5", "SHL-0.51", "SHL-2.0", "SHL-2.1",
    "SMLNJ", "snprintf", "Spencer-86", "Spencer-94", "Spencer-99",
    "SSH-OpenSSH", "SSH-short", "SunPro", "Swift-exception", "SWL", "TCL",
    "TCP-wrappers", "TermReadKey", "TU-Berlin-1.0", "TU-Berlin-2.0", "UCAR",
    "Unicode-DFS-2015", "Unicode-DFS-2016", "UnixCrypt", "Unlicense",
    "UPL-1.0", "Vim", "VSL-1.0", "W3C", "W3C-19980720", "W3C-20150513",
    "w3m", "Widget-Workshop", "Wsuipa", "WTFPL", "X11",
    "X11-distribute-modifications-variant", "Xerox", "Xfig", "XFree86-1.1",
    "xlock", "Xnet", "xpp", "XSkat", "Zed", "Zend-2.0", "Zlib",
    "zlib-acknowledgement", "ZPL-1.1", "ZPL-2.0", "ZPL-2.1",
    # Also include LicenseRef-scancode-* variants that are permissive
    # (Many are listed in the CSV but we'll check by prefix for common ones)
}

# Also add common lowercase variations and common names
PERMISSIVE_LICENSE_NAMES = {
    "mit", "apache-2.0", "apache 2.0", "apache2", "bsd", "isc", "unlicense",
    "wtfpl", "zlib", "public domain", "public-domain", "cc0", "cc0-1.0",
    "bsd-2-clause", "bsd-3-clause", "bsd-4-clause", "artistic-2.0",
    "python-2.0", "postgresql", "json", "curl", "openssl",
}


def classify_license_type(license_name: Optional[str]) -> str:
    """
    Classify license as 'permissive' or 'no_license'.
    
    Args:
        license_name: License name or SPDX ID
        
    Returns:
        'permissive' if license is in the permissive list, 'no_license' otherwise
    """
    if not license_name:
        return "no_license"
    
    # Normalize license name
    license_clean = license_name.strip()
    
    # Check exact match in permissive set
    if license_clean in PERMISSIVE_LICENSE_SPDX_IDS:
        return "permissive"
    
    # Check normalized lowercase match
    license_lower = license_clean.lower()
    if license_lower in PERMISSIVE_LICENSE_NAMES:
        return "permissive"
    
    # Check if it's in the permissive set (case-insensitive)
    for perm_license in PERMISSIVE_LICENSE_SPDX_IDS:
        if perm_license.lower() == license_lower:
            return "permissive"
    
    # Check common patterns
    if any(
        pattern in license_lower
        for pattern in ["mit", "apache", "bsd", "isc", "unlicense", "wtfpl"]
    ):
        # More specific check for common patterns
        if "mit" in license_lower or "apache" in license_lower:
            return "permissive"
        if license_lower.startswith("bsd"):
            return "permissive"
    
    return "no_license"

