import requests
import json

# ============================================================================
# CONFIGURATION - Adjust these settings
# ============================================================================
debugFlag = False
verboseSearchFlag = False  # Set to True to see progressive query execution details

# Progressive Query Settings
# Set the maximum number of results per Elasticsearch query
# Default is 10000 (Elasticsearch limit). Lower values will trigger more aggressive splitting.
# For testing, set to 100 or lower to see the progressive splitting in action.
PROGRESSIVE_QUERY_LIMIT = 10000

idmc_api_version = 20251214
default_infa_url_base = "https://dm-us.informaticacloud.com"
default_infa_hawk_url_base = "https://cdgc-api.dm-us.informaticacloud.com"

# ============================================================================
# PROGRESSIVE QUERY BUILDER - Embedded for single-file distribution
# ============================================================================
"""
Smart Elasticsearch Query Builder with Progressive Splitting

OBJECTS Strategy:
1. Try whole resource
2. If hits limit → split by elementType (OBJECT vs RELATIONSHIP)
3. If OBJECT hits limit → split by asset type (Table, View, Column, etc.)
4. If asset type hits limit → split alphabetically (<a, a-f, f-k, k-p, p-u, u+)
5. If alphabetic range hits limit → split by single letter (<a, a-b, b-c, ..., z+)
6. If single letter hits limit → split by two letters (aa-af, af-ak, ak-ap, ap-au, au-ba)

RELATIONSHIPS Strategy:
1. Try all relationships
2. If hits limit → split alphabetically using wildcards (a-f, f-k, k-p, p-u, u+)
   - Uses wildcards: *.a*, *.A*, *.b*, *.B*, etc.
3. If alphabetic range hits limit → split by single letter (a-b, b-c, c-d, etc.)
   - Uses wildcards: *.a*, *.A*
4. If single letter hits limit → split by two letters (aa, ab, ac, etc.)
   - Uses wildcards: *.aa*, *.aA*, *.Aa*, *.AA*

Notes:
- Elasticsearch normalizes to lowercase for OBJECT range queries
- RELATIONSHIPS use wildcards because script queries don't work reliably
- Special characters (<a) are skipped for relationships (wildcards can't handle them well)
- Each range query uses "gte" (>=) and "lt" (<) for proper boundaries on objects
"""

def build_base_query(origin):
    """Base query structure"""
    return {
        "from": 0,
        "size": 0,
        "query": {
            "bool": {
                "filter": [
                    {"term": {"core.origin": origin}}
                ]
            }
        }
    }

def get_range_label(start, end):
    """Get display label for a range"""
    if start is None and end:
        return f"<{end}"  # Special chars before 'a'
    elif start and end:
        return f"{start}-{end}"
    elif start and not end:
        return f"{start}+"  # Everything >= start
    else:
        return "all"

def add_element_type_filter(query, element_type):
    """Add elementType filter (OBJECT or RELATIONSHIP)"""
    query["query"]["bool"]["filter"].append(
        {"term": {"elementType": element_type}}
    )
    return query

def add_type_filter(query, asset_type):
    """Add specific asset type filter"""
    query["query"]["bool"]["filter"].append(
        {"term": {"type": asset_type}}
    )
    return query

def add_wildcard_filter(query, pattern):
    """Add wildcard filter for name matching"""
    query["query"]["bool"]["filter"].append(
        {"wildcard": {"core.name": pattern}}
    )
    return query

def add_relationship_wildcard_filter(query, start, end):
    """Add wildcard filter for relationship type based on letters in the last segment
    Uses bool should with wildcards like: *.c*, *.C*, *.d*, *.D*, etc.
    """
    wildcards = []
    
    if start is None and end:
        # Special characters before 'a' - can't really handle with wildcards
        # Just skip this range for relationships
        return query
    elif start and end:
        # Normal range: generate wildcards for each letter from start to end (exclusive)
        current = start.lower()
        while current < end.lower():
            # Add both lowercase and uppercase wildcards
            wildcards.append({"wildcard": {"type": f"*.{current}*"}})
            wildcards.append({"wildcard": {"type": f"*.{current.upper()}*"}})
            # Move to next letter
            current = chr(ord(current) + 1)
    elif start and not end:
        # Last range: everything >= start
        # Generate wildcards from start to 'z'
        current = start.lower()
        while current <= 'z':
            wildcards.append({"wildcard": {"type": f"*.{current}*"}})
            wildcards.append({"wildcard": {"type": f"*.{current.upper()}*"}})
            current = chr(ord(current) + 1)
    else:
        # Both None - shouldn't happen
        return query
    
    if wildcards:
        # Add bool should clause with all wildcards
        bool_filter = {
            "bool": {
                "should": wildcards,
                "minimum_should_match": 1
            }
        }
        query["query"]["bool"]["filter"].append(bool_filter)
    
    return query

def add_range_filter(query, start, end):
    """Add alphabetic range filter - uses core.name (without .keyword which doesn't exist in this ES)"""
    # Note: Using core.name may cause duplicates with multi-word names due to tokenization
    # Deduplication is handled in Python after fetching results
    
    if start is None and end:
        # Special range: anything less than 'a' (numbers, underscore, etc.)
        range_filter = {"range": {"core.name": {"lt": end.lower()}}}
    elif start and end:
        # Normal range: start to end
        range_filter = {"range": {"core.name": {"gte": start.lower(), "lt": end.lower()}}}
    elif start and not end:
        # Last range: everything >= start
        range_filter = {"range": {"core.name": {"gte": start.lower()}}}
    else:
        # Both None - shouldn't happen
        return query
    
    query["query"]["bool"]["filter"].append(range_filter)
    return query

def test_query(session, query_dict, limit=10000, description="Query"):
    """Test if query exceeds limit"""
    import json
    query_json = json.dumps(query_dict, indent=2)
    
    # Log the query for debugging
    if False:  # Set to True to see queries
        print(f"\n--- Query: {description} ---")
        print(query_json)
        print("---")
    
    result = session.DG_elastic_search(query_json)
    total = result.get('hits', {}).get('totalHits', 0)
    exceeds_limit = total >= limit
    
    return {
        'total': total,
        'exceeds_limit': exceeds_limit,
        'query': query_dict,
        'description': description,
        'query_json': query_json  # Include for inspection
    }

class ProgressiveQueryBuilder:
    """Builds queries that stay under limit by progressive splitting"""
    
    ASSET_TYPES = [
        "com.infa.odin.models.relational.Database",
        "com.infa.odin.models.relational.Schema", 
        "com.infa.odin.models.relational.Table",
        "com.infa.odin.models.relational.View",
        "com.infa.odin.models.relational.Column",
    ]
    
    # Alphabetic ranges for splitting (lowercase - Elasticsearch normalizes to this)
    # Note: Special characters, numbers, underscore come BEFORE 'a' in ASCII
    ALPHA_RANGES_5 = [
        (None, 'a'),  # Special chars, numbers, underscore, etc.
        ('a', 'f'), ('f', 'k'), ('k', 'p'), ('p', 'u'), ('u', None)
    ]
    
    ALPHA_RANGES_26 = [
        (None, 'a'),  # Special chars before 'a'
        ('a', 'b'), ('b', 'c'), ('c', 'd'), ('d', 'e'), ('e', 'f'),
        ('f', 'g'), ('g', 'h'), ('h', 'i'), ('i', 'j'), ('j', 'k'),
        ('k', 'l'), ('l', 'm'), ('m', 'n'), ('n', 'o'), ('o', 'p'),
        ('p', 'q'), ('q', 'r'), ('r', 's'), ('s', 't'), ('t', 'u'),
        ('u', 'v'), ('v', 'w'), ('w', 'x'), ('x', 'y'), ('y', 'z'),
        ('z', None)  # Everything >= 'z'
    ]
    
    # Two-letter ranges for even deeper splitting (Level 5)
    # For each letter, split into 5 ranges: aa-af, af-ak, ak-ap, ap-au, au-a{next}
    @staticmethod
    def generate_two_letter_ranges(start_letter, end_letter):
        """Generate two-letter ranges for a single letter (e.g., 'a' -> aa-af, af-ak, etc.)"""
        if end_letter is None:
            # Last letter (z) - just do za-zf, zf-zk, etc., zu-None
            return [
                (f'{start_letter}a', f'{start_letter}f'),
                (f'{start_letter}f', f'{start_letter}k'),
                (f'{start_letter}k', f'{start_letter}p'),
                (f'{start_letter}p', f'{start_letter}u'),
                (f'{start_letter}u', None)
            ]
        else:
            # Regular letter - end at the next letter
            return [
                (f'{start_letter}a', f'{start_letter}f'),
                (f'{start_letter}f', f'{start_letter}k'),
                (f'{start_letter}k', f'{start_letter}p'),
                (f'{start_letter}p', f'{start_letter}u'),
                (f'{start_letter}u', end_letter + 'a')  # e.g., au-ba
            ]
    
    def __init__(self, session, origin, limit=10000, verbose=True):
        self.session = session
        self.origin = origin
        self.limit = limit
        self.verbose = verbose
        self.queries = []
        
    def log(self, message):
        if self.verbose:
            print(message)
    
    def build_queries_for_objects(self):
        """Build optimal queries for OBJECT elementType"""
        self.log("\n" + "="*80)
        self.log("BUILDING QUERIES FOR OBJECTS")
        self.log("="*80)
        
        # Level 1: Try all objects at once
        query = build_base_query(self.origin)
        add_element_type_filter(query, "OBJECT")
        result = test_query(self.session, query, self.limit, "All OBJECTS")
        
        self.log(f"\nLevel 1 - All objects: {result['total']:,}")
        
        if not result['exceeds_limit']:
            self.queries.append(result)
            self.log(f"  ✓ Under {self.limit:,} - using single query")
            return
        
        # Level 2: Split by asset type
        self.log(f"  ✗ Exceeds limit - splitting by asset type...")
        
        for asset_type in self.ASSET_TYPES:
            type_name = asset_type.split('.')[-1]
            query = build_base_query(self.origin)
            add_element_type_filter(query, "OBJECT")
            add_type_filter(query, asset_type)
            
            result = test_query(self.session, query, self.limit, f"OBJECT:{type_name}")
            self.log(f"\n  Level 2 - {type_name}: {result['total']:,}")
            
            if not result['exceeds_limit']:
                if result['total'] > 0:  # Only add if has results
                    self.queries.append(result)
                    self.log(f"    ✓ Under {self.limit:,} - added query")
            else:
                # Level 3: Split by alphabetic range
                self.log(f"    ✗ Exceeds limit - splitting alphabetically...")
                self._split_by_alpha(asset_type, type_name)
    
    def _split_by_alpha(self, asset_type, type_name):
        """Split asset type by alphabetic ranges"""
        type_name_short = type_name
        
        # Try 5-way split first
        for start, end in self.ALPHA_RANGES_5:
            range_label = get_range_label(start, end)
            query = build_base_query(self.origin)
            add_element_type_filter(query, "OBJECT")
            add_type_filter(query, asset_type)
            add_range_filter(query, start, end)
            
            result = test_query(self.session, query, self.limit, f"OBJECT:{type_name_short}:{range_label}")
            self.log(f"      Level 3a - {type_name_short} [{range_label}]: {result['total']:,}")
            
            if not result['exceeds_limit']:
                if result['total'] > 0:
                    self.queries.append(result)
                    self.log(f"        ✓ Under {self.limit:,} - added query")
            else:
                # Level 4: Split to single letters
                self.log(f"        ✗ Still exceeds - splitting to single letters...")
                self._split_by_single_letter(asset_type, type_name_short, start, end)
    
    def _split_by_single_letter(self, asset_type, type_name, range_start, range_end):
        """Split to single letter ranges"""
        # Handle special case where range_start is None (for <a range)
        if range_start is None:
            # Can't split special characters further - just add the query
            query = build_base_query(self.origin)
            add_element_type_filter(query, "OBJECT")
            add_type_filter(query, asset_type)
            add_range_filter(query, None, range_end)
            result = test_query(self.session, query, self.limit, f"OBJECT:{type_name}:<{range_end}")
            self.queries.append(result)
            self.log(f"            ⚠️  WARNING: Special characters range [{result['total']:,}] cannot be split further!")
            return
        
        # Get relevant single-letter ranges
        relevant_ranges = [
            (s, e) for s, e in self.ALPHA_RANGES_26 
            if s is not None and s >= range_start and (range_end is None or s < range_end)
        ]
        
        for start, end in relevant_ranges:
            range_label = get_range_label(start, end)
            query = build_base_query(self.origin)
            add_element_type_filter(query, "OBJECT")
            add_type_filter(query, asset_type)
            add_range_filter(query, start, end)
            
            result = test_query(self.session, query, self.limit, f"OBJECT:{type_name}:{range_label}")
            self.log(f"          Level 4 - {type_name} [{range_label}]: {result['total']:,}")
            
            if result['total'] > 0:
                if result['exceeds_limit']:
                    # Level 5: Split by two-letter ranges
                    self.log(f"            ✗ Still exceeds {self.limit:,} - splitting to two-letter ranges...")
                    self._split_by_two_letters(asset_type, type_name, start, end)
                else:
                    self.queries.append(result)
                    self.log(f"            ✓ Under {self.limit:,} - added query")
    
    def _split_by_two_letters(self, asset_type, type_name, letter_start, letter_end):
        """Split to two-letter ranges (aa-af, af-ak, etc.)"""
        # Generate two-letter ranges for this letter
        two_letter_ranges = self.generate_two_letter_ranges(letter_start, letter_end)
        
        for start, end in two_letter_ranges:
            range_label = get_range_label(start, end)
            query = build_base_query(self.origin)
            add_element_type_filter(query, "OBJECT")
            add_type_filter(query, asset_type)
            add_range_filter(query, start, end)
            
            result = test_query(self.session, query, self.limit, f"OBJECT:{type_name}:{range_label}")
            self.log(f"              Level 5 - {type_name} [{range_label}]: {result['total']:,}")
            
            if result['total'] > 0:
                self.queries.append(result)
                if result['exceeds_limit']:
                    self.log(f"                ⚠️  CRITICAL: Still exceeds {self.limit:,} after two-letter split!")
                else:
                    self.log(f"                ✓ Under {self.limit:,} - added query")
    
    def build_queries_for_relationships(self):
        """Build queries for RELATIONSHIP elements with progressive splitting"""
        self.log("\n" + "="*80)
        self.log("BUILDING QUERIES FOR RELATIONSHIPS")
        self.log("="*80)
        
        # First, try all relationships
        query = build_base_query(self.origin)
        add_element_type_filter(query, "RELATIONSHIP")
        
        result = test_query(self.session, query, self.limit, "All RELATIONSHIPS")
        self.log(f"\nAll relationships: {result['total']:,}")
        
        if result['total'] == 0:
            return
        
        if not result['exceeds_limit']:
            # All relationships fit in one query
            self.queries.append(result)
            self.log(f"  ✓ Under {self.limit:,} - added query")
        else:
            # Split alphabetically by relationship type name (last segment after dots)
            self.log(f"  ✗ Exceeds {self.limit:,} - splitting by relationship type name...")
            self._split_relationships_by_alpha()
    
    def _split_relationships_by_alpha(self):
        """Split relationships alphabetically by their type name (using wildcards)"""
        # Try 5-way split first
        for start, end in self.ALPHA_RANGES_5:
            # Skip special characters range for relationships (can't wildcard it effectively)
            if start is None:
                continue
                
            range_label = get_range_label(start, end)
            query = build_base_query(self.origin)
            add_element_type_filter(query, "RELATIONSHIP")
            add_relationship_wildcard_filter(query, start, end)
            
            result = test_query(self.session, query, self.limit, f"RELATIONSHIP:{range_label}")
            self.log(f"    Level 1 - Relationships [{range_label}]: {result['total']:,}")
            
            if result['total'] > 0:
                if result['exceeds_limit']:
                    # Split to single letters
                    self.log(f"      ✗ Exceeds {self.limit:,} - splitting to single letters...")
                    self._split_relationships_by_single_letter(start, end)
                else:
                    self.queries.append(result)
                    self.log(f"      ✓ Under {self.limit:,} - added query")
    
    def _split_relationships_by_single_letter(self, range_start, range_end):
        """Split relationships to single letter ranges (using wildcards)"""
        # Get relevant single-letter ranges
        relevant_ranges = [
            (s, e) for s, e in self.ALPHA_RANGES_26 
            if s is not None and s >= range_start and (range_end is None or s < range_end)
        ]
        
        for start, end in relevant_ranges:
            # Skip special characters range
            if start is None:
                continue
                
            range_label = get_range_label(start, end)
            query = build_base_query(self.origin)
            add_element_type_filter(query, "RELATIONSHIP")
            add_relationship_wildcard_filter(query, start, end)
            
            result = test_query(self.session, query, self.limit, f"RELATIONSHIP:{range_label}")
            self.log(f"        Level 2 - Relationships [{range_label}]: {result['total']:,}")
            
            if result['total'] > 0:
                if result['exceeds_limit']:
                    # Level 3: Split by two-letter wildcards
                    self.log(f"          ✗ Still exceeds {self.limit:,} - splitting to two-letter wildcards...")
                    self._split_relationships_by_two_letters(start, end)
                else:
                    self.queries.append(result)
                    self.log(f"          ✓ Under {self.limit:,} - added query")
    
    def _split_relationships_by_two_letters(self, letter_start, letter_end):
        """Split relationships by two-letter wildcards (aa, ab, ac, etc.)"""
        # Generate two-letter ranges for this letter
        two_letter_ranges = self.generate_two_letter_ranges(letter_start, letter_end)
        
        for start, end in two_letter_ranges:
            range_label = get_range_label(start, end)
            query = build_base_query(self.origin)
            add_element_type_filter(query, "RELATIONSHIP")
            
            # For two-letter ranges, we need to generate wildcards for each two-letter combo
            wildcards = []
            if end:
                # Generate all two-letter combinations in this range
                current = start.lower()
                while current < end.lower():
                    # Add all case variations: aa, aA, Aa, AA
                    for c1 in [current[0].lower(), current[0].upper()]:
                        for c2 in [current[1].lower(), current[1].upper()]:
                            wildcards.append({"wildcard": {"type": f"*.{c1}{c2}*"}})
                    
                    # Move to next two-letter combo (increment second letter)
                    if current[1] < 'z':
                        current = current[0] + chr(ord(current[1]) + 1)
                    else:
                        # Wrap to next first letter
                        break
            else:
                # Last range - just add this one combo
                for c1 in [start[0].lower(), start[0].upper()]:
                    for c2 in [start[1].lower(), start[1].upper()]:
                        wildcards.append({"wildcard": {"type": f"*.{c1}{c2}*"}})
            
            if wildcards:
                bool_filter = {
                    "bool": {
                        "should": wildcards,
                        "minimum_should_match": 1
                    }
                }
                query["query"]["bool"]["filter"].append(bool_filter)
            
            result = test_query(self.session, query, self.limit, f"RELATIONSHIP:{range_label}")
            self.log(f"            Level 3 - Relationships [{range_label}]: {result['total']:,}")
            
            if result['total'] > 0:
                self.queries.append(result)
                if result['exceeds_limit']:
                    self.log(f"              ⚠️  CRITICAL: Still exceeds {self.limit:,} after two-letter split!")
                else:
                    self.log(f"              ✓ Under {self.limit:,} - added query")
    
    def build_all_queries(self):
        """Build all necessary queries"""
        self.build_queries_for_objects()
        self.build_queries_for_relationships()
        
        self.log("\n" + "="*80)
        self.log(f"FINAL QUERY PLAN: {len(self.queries)} queries")
        self.log("="*80)
        total_results = sum(q['total'] for q in self.queries)
        self.log(f"Total results to fetch: {total_results:,}")
        
        for i, q in enumerate(self.queries, 1):
            self.log(f"  {i:2d}. {q['description']:40s} - {q['total']:>6,} results")
        
        return self.queries


if __name__ == "__main__":
    print("This module provides progressive query splitting.")
    print("Import and use ProgressiveQueryBuilder class.")

# ============================================================================
# END OF PROGRESSIVE QUERY BUILDER
# ============================================================================


class INFA_DG_Object:

    def debug(self, message):
        if debugFlag:
            print(f"DEBUG: {message}")

    def getvalue(self, key):
        return self.map[key]
    
    def fetchOtherRelationships(self):

        ## Search for Glossary Relatopnsips
        payload_dict = {
            "from": 0,
            "size": 100,
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"elementType": "RELATIONSHIP" }},
                        {"term": {"type":"com.infa.ccgf.models.governance.IClassTechnicalGlossaryBase" }}
                        
                    ]
                }
            },
            "sort": [
                {
                    "com.infa.ccgf.models.governance.scannedTime": {
                        "order": "desc"
                    }
                }
            ]
        }

        results = self.session.elasticSearchResults(payload_dict)

        self.debug(f"Total results from fetchOtherRelationships elasticsearch: {len(results)}")

        for search_obj in results:
            
            try:
                raw_map = search_obj['sourceAsMap']
                if search_obj['sourceAsMap']['elementType'] == 'RELATIONSHIP' and 'ACCEPTED' in raw_map['core.curationStatus'] and ( self.origin == raw_map['core.sourceOrigin']  or self.origin == raw_map['core.targetOrigin']):

                    source_id = raw_map['core.sourceIdentity']
                    target_id = raw_map['core.targetIdentity']

                    source_obj = self.session.getObjectByID(source_id)
                    target_obj = self.session.getObjectByID(target_id)
                    
                    
                    if target_obj.classType == 'core.DataElementClassification' or target_obj.classType == 'core.DataEntityClassification':
                        if not target_obj in source_obj.classifications:
                            source_obj.classifications.append(target_obj)
                    elif target_obj.classType == 'com.infa.ccgf.models.governance.BusinessTerm':
                        if not target_obj in source_obj.businessterms:
                            source_obj.businessterms.append(target_obj)
            except:
                pass        

        ## Search for Classification Relatopnsips
        payload_dict = {
            "from": 0,
            "size": 100,
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"elementType": "RELATIONSHIP" }},
                        {"term": {"type":"core.ClassifiedAs" }}
                        
                    ]
                }
            },
            "sort": [
                {
                    "com.infa.ccgf.models.governance.scannedTime": {
                        "order": "desc"
                    }
                }
            ]
        }

        results = self.session.elasticSearchResults(payload_dict)

        self.debug(f"Total results from fetchOtherRelationships elasticsearch: {len(results)}")

        for search_obj in results:
            
            try:
                raw_map = search_obj['sourceAsMap']
                if search_obj['sourceAsMap']['elementType'] == 'RELATIONSHIP' and 'ACCEPTED' in raw_map['core.curationStatus'] and ( self.origin == raw_map['core.sourceOrigin']  or self.origin == raw_map['core.targetOrigin']):
                    source_id = raw_map['core.sourceIdentity']
                    target_id = raw_map['core.targetIdentity']

                    source_obj = self.session.getObjectByID(source_id)
                    target_obj = self.session.getObjectByID(target_id)
                    
                    
                    if target_obj.classType == 'core.DataElementClassification' or target_obj.classType == 'core.DataEntityClassification':
                        if not target_obj in source_obj.classifications:
                            source_obj.classifications.append(target_obj)
                    elif target_obj.classType == 'com.infa.ccgf.models.governance.BusinessTerm':
                        if not target_obj in source_obj.businessterms:
                            source_obj.businessterms.append(target_obj)
            except:
                pass

    def fetchObjects(self, use_progressive=True, limit=None, verbose=None):
        """
        Fetch all objects and relationships for this resource.
        
        Args:
            use_progressive: Use progressive query splitting (default True)
            limit: Maximum results per query (default: PROGRESSIVE_QUERY_LIMIT)
            verbose: Print progress messages (default: verboseSearchFlag global setting)
        """
        if limit is None:
            limit = PROGRESSIVE_QUERY_LIMIT
        if verbose is None:
            verbose = verboseSearchFlag
            
        if use_progressive:
            # Progressive mode - uses ProgressiveQueryBuilder
            print(f"INFO: Fetching Detailed Information for {self.name} (Progressive Mode)")
            
            # Build progressive queries
            builder = ProgressiveQueryBuilder(self.session, self.origin, limit=limit, verbose=verbose)
            queries = builder.build_all_queries()
            
            if verbose:
                print(f"\nINFO: Executing {len(queries)} progressive queries...")
            else:
                print(f"INFO: Executing {len(queries)} progressive queries...")
            
            # Execute all queries and collect results
            all_results = []
            for i, query_info in enumerate(queries, 1):
                if verbose:
                    print(f"  Fetching Query {i}/{len(queries)}: {query_info['description']} (expecting ~{query_info['total']:,} results)...")
                
                # Create a deep copy of the query and prepare for fetching
                import copy
                query_dict = copy.deepcopy(query_info['query'])
                # Ensure query structure is clean for elasticSearchResults
                # elasticSearchResults will set 'from' and 'size' itself
                if 'from' in query_dict:
                    del query_dict['from']
                if 'size' in query_dict:
                    del query_dict['size']
                
                # Execute this query with pagination
                results = self.session.elasticSearchResults(query_dict)
                
                if verbose:
                    print(f"    Retrieved {len(results)} results")
                
                self.debug(f"    Query {i}: Retrieved {len(results)} results (expected {query_info['total']})")
                all_results.extend(results)
            
            print(f"INFO: Total results collected: {len(all_results):,}")
            
            # Deduplicate by creating unique key based on element type
            # OBJECTS use core.identity, RELATIONSHIPS use source+target+type
            seen_keys = set()
            deduplicated_results = []
            for result in all_results:
                source_map = result.get('sourceAsMap', {})
                element_type = source_map.get('elementType')
                
                # Create unique key based on element type
                if element_type == 'OBJECT':
                    # Objects use their identity
                    unique_key = source_map.get('core.identity')
                elif element_type == 'RELATIONSHIP':
                    # Relationships use combination of source + target + type
                    source_id = source_map.get('core.sourceIdentity', '')
                    target_id = source_map.get('core.targetIdentity', '')
                    rel_type = source_map.get('type', '')
                    unique_key = f"REL:{source_id}:{target_id}:{rel_type}"
                else:
                    # Fallback for unknown types
                    unique_key = source_map.get('core.identity') or str(hash(str(source_map)))
                
                if unique_key and unique_key not in seen_keys:
                    seen_keys.add(unique_key)
                    deduplicated_results.append(result)
            
            if verbose and len(all_results) != len(deduplicated_results):
                print(f"INFO: Deduplicated: {len(all_results):,} → {len(deduplicated_results):,} results")
            
            all_results = deduplicated_results
            
            self.debug(f"Total results from fetchObjects (progressive) elasticsearch: {len(all_results)}")
            
        else:
            # Original single-query mode
            print(f"INFO: Fetching Detailed Information for {self.name}")
            payload_dict = {
                "from": 0,
                "size": 1000,
                "query": {
                    "term": {
                        "core.origin": self.origin
                    }
                },
                "sort": [
                    {
                        "com.infa.ccgf.models.governance.scannedTime": {
                            "order": "desc"
                        }
                    }
                ]
            }
            
            all_results = self.session.elasticSearchResults(payload_dict)
            self.debug(f"Total results from fetchObjects elasticsearch: {len(all_results)}")
        
        # Process results (same for both modes)
        # First pass: Create OBJECT instances
        for obj in all_results:
            try:
                if obj['sourceAsMap']['elementType'] == 'OBJECT':
                    raw_map = obj['sourceAsMap']
                    
                    object = INFA_DG_Object(self.session, raw_map)
                    self.objects.append(object)
                    self.session.all_objects.append(object)
            except:
                pass

        # Second pass: Process RELATIONSHIP instances
        for obj in all_results:
            try:
                if obj['sourceAsMap']['elementType'] == 'RELATIONSHIP':
                    raw_map = obj['sourceAsMap']
                    source_id = raw_map['core.sourceIdentity']
                    target_id = raw_map['core.targetIdentity']

                    target_obj = None
                    source_obj = None
                    for o in self.session.all_objects:
                        if o.identity == source_id:
                            source_obj = o
                        if o.identity == target_id:
                            target_obj = o

                    try:
                        if obj['sourceAsMap']['core.associationKind'] == "core.ParentChild":
                            if source_obj != None and target_obj != None:
                                source_obj.child_objects.append(target_obj)
                                target_obj.parent_objects.append(source_obj)
                    except:
                        pass

                    '''
                    if source_id == '9b7f40c3-c1fe-45e5-9c8c-c945b11c4c2d' or target_id == '9b7f40c3-c1fe-45e5-9c8c-c945b11c4c2d':
                        print(f"DEBUG** {source_obj.name} ({source_obj.externalId}) --> {target_obj.name} ({target_obj.externalId})")
                    '''
                    
                    if 'ACCEPTED' in raw_map['core.curationStatus'] and (target_obj.classType == 'core.DataElementClassification' or target_obj.classType == 'core.DataEntityClassification'):
                        source_obj.classifications.append(target_obj)
                    elif 'ACCEPTED' in raw_map['core.curationStatus'] and (target_obj.classType == 'com.infa.ccgf.models.governance.BusinessTerm'):
                        source_obj.businessterms.append(target_obj)
            except:
                pass
        
        ## Don't need to run this, as we've already got all of the relationships above.
        self.fetchOtherRelationships()


    def getObjectsByShortType(self, shortType):
        result_array = []
        for i in self.objects:
            if i.shortType.lower() == shortType.lower():
                result_array.append(i)
        return result_array


    def getObjectsByType(self, classType):
        result_array = []
        for i in self.objects:
            if i.classType == classType:
                result_array.append(i)
        return result_array
    
    def getClassificationNames(self):
        clist = []
        for o in self.classifications:
            clist.append(o.name)
        return ','.join(clist)
    
    def getBusinessTermNames(self):
        blist = []
        for o in self.businessterms:
            blist.append(o.name)
        return ','.join(blist)    

    def getFriendlyId(self):
        try:
            session = self.session
            originFriendlyName = session.getObjectByLocationID(self.origin).name
            friendlyId = self.externalId.split('~')[0].replace(self.origin, originFriendlyName)
            return friendlyId
        except:
            print("Error getting friendly name session.getObjectByLocationID("+self.origin+").name")

    def getAllRelatedPolicies(self):
        result_array = []
        for obj in self.parentPolicies:
            result_array.append(obj.name)
        
        for bt in self.businessterms:
            for bt_parentpolicy in bt.parentPolicies:
                result_array.append(bt_parentpolicy.name)

        for cl in self.classifications:
             for cl_parentpolicy in cl.parentPolicies:
                result_array.append(cl_parentpolicy.name)

        for obj in self.child_objects:
            child_array =  obj.getAllRelatedPolicies()
            result_array = result_array + child_array

        ## Remove duplicates
        result_array = list(set(result_array))

        return result_array
        

    def getParentPolicyNames(self):
        '''
        result_array = []
        for obj in self.parentPolicies:
            result_array.append(obj.name)
        
        for bt in self.businessterms:
            for bt_parentpolicy in bt.parentPolicies:
                result_array.append(bt_parentpolicy.name)

        for cl in self.classifications:
             for cl_parentpolicy in cl.parentPolicies:
                result_array.append(cl_parentpolicy.name)           

        ## Remove duplicates
        result_array = list(set(result_array))
        '''
        result_array = self.getAllRelatedPolicies()
        return ','.join(result_array)

    def __init__(self, session, raw_map ):
        self.classifications = []
        self.businessterms = []
        self.objects = []
        self.parent_objects = []
        self.child_objects = []        
        self.session = session
        self.name = raw_map['core.name']
        self.isResource = False
        self.isDataSet = False
        self.isDataElement = False
        try:
            for x in raw_map['type']:
                if x == "core.DataElement":
                    self.isDataElement = True
                if x == "core.DataSet":
                    self.isDataSet = True                    
        except:
            pass 
        self.map = raw_map
        self.description = ""
        try:
            self.description = self.getvalue('core.description')
        except:
            pass        
        self.origin = self.getvalue('core.origin')
        self.externalId = self.getvalue('core.externalId')
        self.classType = self.getvalue('core.classType')
        self.shortType = self.classType.split('.')[-1]
        self.elementType = self.getvalue('elementType')
        self.identity = self.getvalue('core.identity')
        self.parentPolicies = []





class INFASession:

    def debug(self, message):
        if debugFlag:
            print(f"DEBUG: {message}")    

    def get_sessionid_and_orgid(self, username, password):
        url = self.url_base+'/identity-service/api/v1/Login'
        d = {}
        d['username'] = username
        d['password'] = password
        payload = json.dumps(d)
        headers = {
        'Content-Type': 'application/json'
        }


        self.debug(f"idmc_api.get_sessionid_and_orgid: Running API: {url} with payload: {payload}")
        response = requests.request("POST", url, headers=headers, data=payload)
        response_data = response.json()
        session_id = response_data['sessionId']
        org_id = response_data['currentOrgId']

        return session_id, org_id

    def get_token(self,session_id, org_id):
        url = self.url_base+"/identity-service/api/v1/jwt/Token?client_id=cdlg_app&nonce=gxx3t69BWB49BHHNn&access_code="

        payload = ""
        headers = {}
        headers['Content-Type'] = 'application/json'
        headers['IDS-SESSION-ID'] = session_id
        headers['X-INFA-ORG-ID'] =  org_id

        self.debug(f"idmc_api.get_token: Running API: {url}")
        response = requests.request("GET", url, headers=headers, data=payload)
        response_data = response.json()

        token = response_data['jwt_token']
        return token

    def DG_elastic_search(self, json_query):
        url = self.hawk_url_base+"/ccgf-searchv2/api/v1/search"
        payload = json_query 
        headers = {}
        headers['Content-Type'] = 'application/json'
        headers['X-INFA-SEARCH-LANGUAGE'] = 'elasticsearch'
        headers['X-INFA-ORG-ID'] =  self.org_id
        headers['Authorization'] =  'Bearer '+self.token

        self.debug(f"idmc_api.DG_elastic_search: About to call {url} Payload: {payload}")

        response = requests.request("POST", url, headers=headers, data=payload)

        self.debug(f"idmc_api.DG_elastic_search: Raw Response for {url}:")
        self.debug(f"{response.text}")
        return response.json()
    
    def elasticSearchResults(self, payload_dict):
        ## Calling multipl elastic searches to deal with pagination.
        all_results = []
        from_offset = payload_dict.get('from', 0)
        size = payload_dict.get('size', 100)  # default page size

        while True:
            payload_dict['from'] = from_offset
            payload_dict['size'] = size

            payload = json.dumps(payload_dict)
            response = self.DG_elastic_search(payload)

            results = response['hits']['hits']
            total_hits = response['hits']['totalHits']
            total_hits_this_page = len(results)

            all_results.extend(results)
            
            from_offset += total_hits_this_page
            
            # Break if no more hits or we retrieved all
            if total_hits_this_page == 0 or from_offset >= total_hits:
                break

        return all_results    

    def DG_publish(self, json_payload):
        url = self.hawk_url_base+"/ccgf-contentv2/api/v1/publish"
        payload = json_payload 
        headers = {}
        headers['Content-Type'] = 'application/json'
        headers['X-INFA-SEARCH-LANGUAGE'] = 'elasticsearch'
        headers['X-INFA-PRODUCT-ID'] = 'CDGC'
        headers['X-INFA-ORG-ID'] =  self.org_id
        headers['Authorization'] =  'Bearer '+self.token

        self.debug("publish: "+json_payload)
        response = requests.request("POST", url, headers=headers, data=payload)
        return response.json()

    def deleteById(self, obj_identity):

        o = self.getObjectByID(obj_identity)
        classType = o.classType
        elementType = o.elementType
        payload = json.dumps({
        "items": [
            {
            "elementType": elementType,
            "identity": obj_identity,
            "operation": "DELETE",
            "type": classType,
            "identityType": "INTERNAL",
            "attributes": {}
            }
        ]
        })

        return self.DG_publish(payload)       

    def _fetchObjectsByClassType_progressive(self, class_type, limit=None, verbose=None):
        """
        Helper method to fetch all objects of a specific classType using progressive queries.
        Uses alphabetic splitting if results exceed the limit.
        
        Args:
            class_type: The core.classType to fetch (e.g., "core.Resource")
            limit: Maximum results per query (default: PROGRESSIVE_QUERY_LIMIT)
            verbose: Print progress messages (default: verboseSearchFlag global setting)
            
        Returns:
            List of raw Elasticsearch results (hits)
        """
        if limit is None:
            limit = PROGRESSIVE_QUERY_LIMIT
        if verbose is None:
            verbose = verboseSearchFlag
            
        if verbose:
            print(f"INFO: Fetching objects of type {class_type} (Progressive Mode, limit={limit:,})")
        
        # Try to fetch all in one query first
        test_query = {
            "from": 0,
            "size": 0,  # Just get count
            "query": {
                "term": {
                    "core.classType": class_type
                }
            }
        }
        
        test_result = self.DG_elastic_search(json.dumps(test_query))
        total_count = test_result.get('hits', {}).get('totalHits', 0)
        
        if verbose:
            print(f"  Total {class_type} objects: {total_count:,}")
        
        if total_count == 0:
            return []
        
        if total_count < limit:
            # Fits in one query
            if verbose:
                print(f"  ✓ Under {limit:,} - using single query")
            query_dict = {
                "from": 0,
                "size": 10000,
                "query": {
                    "term": {
                        "core.classType": class_type
                    }
                }
            }
            return self.elasticSearchResults(query_dict)
        
        # Need to split - use alphabetic ranges on core.name
        if verbose:
            print(f"  ✗ Exceeds {limit:,} - splitting alphabetically...")
        
        all_results = []
        alpha_ranges = [
            (None, 'a'), ('a', 'f'), ('f', 'k'), ('k', 'p'), ('p', 'u'), ('u', None)
        ]
        
        for start, end in alpha_ranges:
            query_dict = {
                "from": 0,
                "size": 10000,
                "query": {
                    "bool": {
                        "filter": [
                            {"term": {"core.classType": class_type}}
                        ]
                    }
                }
            }
            
            # Add range filter
            # Add range filter (note: core.name may cause duplicates due to tokenization)
            if start is None and end:
                range_filter = {"range": {"core.name": {"lt": end.lower()}}}
            elif start and end:
                range_filter = {"range": {"core.name": {"gte": start.lower(), "lt": end.lower()}}}
            elif start and not end:
                range_filter = {"range": {"core.name": {"gte": start.lower()}}}
            else:
                continue
            
            query_dict["query"]["bool"]["filter"].append(range_filter)
            
            # Get count for this range
            count_query = query_dict.copy()
            count_query["size"] = 0
            count_result = self.DG_elastic_search(json.dumps(count_query))
            range_count = count_result.get('hits', {}).get('totalHits', 0)
            
            if range_count == 0:
                continue
            
            range_label = f"<{end}" if start is None else f"{start}-{end}" if end else f"{start}+"
            
            if verbose:
                print(f"    Range [{range_label}]: {range_count:,} results")
            
            # Fetch results for this range
            results = self.elasticSearchResults(query_dict)
            all_results.extend(results)
        
        # Deduplicate by core.identity (may have duplicates due to tokenization of multi-word names)
        seen_ids = set()
        deduplicated_results = []
        for result in all_results:
            identity = result.get('sourceAsMap', {}).get('core.identity')
            if identity and identity not in seen_ids:
                seen_ids.add(identity)
                deduplicated_results.append(result)
        
        if verbose and len(all_results) != len(deduplicated_results):
            print(f"  Deduplicated: {len(all_results):,} → {len(deduplicated_results):,} results")
        
        if verbose:
            print(f"  Total results collected: {len(deduplicated_results):,}")
        
        return deduplicated_results


    def fetchResources(self, use_progressive=True, verbose=None):
        """
        Fetch all resources. 
        
        Args:
            use_progressive: Use progressive query splitting (default True)
            verbose: Print progress messages (default: verboseSearchFlag global setting)
        """
        if verbose is None:
            verbose = verboseSearchFlag
        if use_progressive:
            results = self._fetchObjectsByClassType_progressive(
                "core.Resource", 
                limit=PROGRESSIVE_QUERY_LIMIT, 
                verbose=verbose
            )
        else:
            # Original single-query method
            payload_dict = {
                "from": 0,
                "size": 10000,
                "query": {
                    "term": {
                        "core.classType": "core.Resource"
                    }
                },
                "sort": [
                    {
                        "com.infa.ccgf.models.governance.scannedTime": {
                            "order": "desc"
                        }
                    }
                ]
            }
            results = self.elasticSearchResults(payload_dict)

        self.debug(f"Total results from fetchResources elasticsearch: {len(results)}")

        for obj in results:
            raw_map = obj['sourceAsMap']
            try:
                resource = INFA_DG_Object(self, raw_map)
                resource.isResource = True
                self.resources.append(resource)
            except:
                pass

    def fetchClassifications(self, use_progressive=True, verbose=None):
        """
        Fetch all classifications. 
        
        Args:
            use_progressive: Use progressive query splitting (default True)
            verbose: Print progress messages (default: verboseSearchFlag global setting)
        """
        if verbose is None:
            verbose = verboseSearchFlag
        
        self.fetchParentPolicyOfClassifications()

        if use_progressive:
            results = self._fetchObjectsByClassType_progressive(
                "core.DataElementClassification", 
                limit=PROGRESSIVE_QUERY_LIMIT, 
                verbose=verbose
            )
        else:
            # Original single-query method
            payload_dict = {
                "from": 0,
                "size": 1000,
                "query": {
                    "term": {
                        "core.classType": "core.DataElementClassification"
                    }
                },
                "sort": [
                    {
                        "com.infa.ccgf.models.governance.scannedTime": {
                            "order": "desc"
                        }
                    }
                ]
            }
            results = self.elasticSearchResults(payload_dict)

        self.debug(f"Total results from fetchClassifications (DataElementClassification) elasticsearch: {len(results)}")

        for obj in results:
            raw_map = obj['sourceAsMap']
            try:
                classification = INFA_DG_Object(self, raw_map)
                classification.parentPolicies = self.fetchParentPolicyOfClassification(classification.identity)
                self.classifications.append(classification)
                self.all_objects.append(classification)
            except:
                pass


        payload_dict = {
        "from": 0,
        "size": 1000,
        "query": {
            "term": {
            "core.classType": "core.DataEntityClassification"
            }
        },
        "sort": [
            {
            "com.infa.ccgf.models.governance.scannedTime": {
                "order": "desc"
            }
            }
        ]
        }

        results = self.elasticSearchResults(payload_dict)

        self.debug(f"Total results from fetchClassifications (DataEntityClassification) elasticsearch: {len(results)}")

        for obj in results:
            raw_map = obj['sourceAsMap']
            try:
                classification = INFA_DG_Object(self, raw_map)
                self.classifications.append(classification)
                self.all_objects.append(classification)
            except:
                pass            

    def fetchParentPolicyOfClassifications(self):

        ## result_objects = []

        payload_dict = {
        "from": 0,
        "size": 10000,
        "query": {
            "bool": {
                "filter": [
                    {"term": {"elementType": "RELATIONSHIP" }},
                    {"term": {"type":"com.infa.ccgf.models.governance.relatedPolicyClassification" }}
                ]
            }
        },
        "sort": [
            {
            "com.infa.ccgf.models.governance.scannedTime": {
                "order": "desc"
            }
            }
        ]
        }

        results = self.elasticSearchResults(payload_dict)

        self.debug(f"Total results from fetchParentPolicyOfClassifications elasticsearch: {len(results)}")


        search_results = results
        for res in search_results:
            this_source_identity = res['sourceAsMap']['core.sourceIdentity']
            this_target_identity = res['sourceAsMap']['core.targetIdentity']
            this_relationship = {"name": this_source_identity+" "+this_target_identity, "source_identity": this_source_identity, "target_identity": this_target_identity}
            self.all_relationships.append(this_relationship)


    def fetchParentPolicyOfClassification(self, classification_id):
        result_objects = []
        for rel in self.all_relationships:
            source_identity = rel["source_identity"]
            target_identity = rel["target_identity"]
            if target_identity == classification_id:
                for pol in self.policies:
                    if pol.identity == source_identity:
                        self.debug(f"Adding policy {pol.name} as a parent of Classification with ID of {classification_id}")
                        result_objects.append(pol)

        return result_objects

    def fetchParentPolicyOfBusinessTerms(self):

        ## result_objects = []

        payload_dict = {
        "from": 0,
        "size": 10000,
        "query": {
            "bool": {
                "filter": [
                    {"term": {"elementType": "RELATIONSHIP" }},
                    {"term": {"type":"com.infa.ccgf.models.governance.relatedBusinessTermPolicy" }}
                ]
            }
        },
        "sort": [
            {
            "com.infa.ccgf.models.governance.scannedTime": {
                "order": "desc"
            }
            }
        ]
        }

        results = self.elasticSearchResults(payload_dict)

        self.debug(f"Total results from fetchParentPolicyOfBusinessTerms elasticsearch: {len(results)}")

        for res in results:
            this_source_identity = res['sourceAsMap']['core.sourceIdentity']
            this_target_identity = res['sourceAsMap']['core.targetIdentity']
            this_relationship = {"name": this_source_identity+" "+this_target_identity, "source_identity": this_source_identity, "target_identity": this_target_identity}
            self.all_relationships.append(this_relationship)


    def fetchParentPolicyOfBusinessTerm(self, business_term_id):
        
        result_objects = []
        for rel in self.all_relationships:
            source_identity = rel["source_identity"]
            target_identity = rel["target_identity"]
            if target_identity == business_term_id:
                for pol in self.policies:
                    if pol.identity == source_identity:
                        self.debug(f"Adding policy {pol.name} as a parent of Business Term with ID of {business_term_id}")
                        result_objects.append(pol)

        return result_objects

    def fetchPolicies(self, use_progressive=True, verbose=None):
        """
        Fetch all policies. 
        
        Args:
            use_progressive: Use progressive query splitting (default True)
            verbose: Print progress messages (default: verboseSearchFlag global setting)
        """
        if verbose is None:
            verbose = verboseSearchFlag
        if use_progressive:
            results = self._fetchObjectsByClassType_progressive(
                "com.infa.ccgf.models.governance.Policy", 
                limit=PROGRESSIVE_QUERY_LIMIT, 
                verbose=verbose
            )
        else:
            # Original single-query method
            payload_dict = {
                "from": 0,
                "size": 10000,
                "query": {
                    "term": {
                        "core.classType": "com.infa.ccgf.models.governance.Policy"
                    }
                },
                "sort": [
                    {
                        "com.infa.ccgf.models.governance.scannedTime": {
                            "order": "desc"
                        }
                    }
                ]
            }
            results = self.elasticSearchResults(payload_dict)

        self.debug(f"Total results from fetchPolicies elasticsearch: {len(results)}")        

        for obj in results:
            raw_map = obj['sourceAsMap']
            try:
                pol = INFA_DG_Object(self, raw_map)
                self.policies.append(pol)
                self.all_objects.append(pol)
            except:
                pass

    def fetchBusinessTerms(self, use_progressive=True, verbose=None):
        """
        Fetch all business terms. 
        
        Args:
            use_progressive: Use progressive query splitting (default True)
            verbose: Print progress messages (default: verboseSearchFlag global setting)
        """
        if verbose is None:
            verbose = verboseSearchFlag
        
        self.fetchParentPolicyOfBusinessTerms()

        if use_progressive:
            results = self._fetchObjectsByClassType_progressive(
                "com.infa.ccgf.models.governance.BusinessTerm", 
                limit=PROGRESSIVE_QUERY_LIMIT, 
                verbose=verbose
            )
        else:
            # Original single-query method
            payload_dict = {
                "from": 0,
                "size": 10000,
                "query": {
                    "term": {
                        "core.classType": "com.infa.ccgf.models.governance.BusinessTerm"
                    }
                },
                "sort": [
                    {
                        "com.infa.ccgf.models.governance.scannedTime": {
                            "order": "desc"
                        }
                    }
                ]
            }
            results = self.elasticSearchResults(payload_dict)

        self.debug(f"Total results from fetchBusinessTerms elasticsearch: {len(results)}")        

        for obj in results:
            raw_map = obj['sourceAsMap']
            try:
                term = INFA_DG_Object(self, raw_map)
                term.parentPolicies = self.fetchParentPolicyOfBusinessTerm(term.identity)

                self.businessterms.append(term)
                self.all_objects.append(term)
            except:
                pass

    def getObjectByID(self, identity):

        for o in self.all_objects:
            if o.identity == identity:
                return o

        payload_dict = {
        "from": 0,
        "size": 10000,
        "query": {
            "bool": {
            "filter": [
                {
                "term": {
                    "core.identity": identity
                }
                }
            ]
            }
        },
        "sort": [
            {
            "com.infa.ccgf.models.governance.scannedTime": {
                "order": "desc"
            }
            }
        ]
        }

        results = self.elasticSearchResults(payload_dict)

        self.debug(f"Total results from getObjectByID elasticsearch: {len(results)}")

        for obj in results:
            raw_map = obj['sourceAsMap']
            try:
                o = INFA_DG_Object(self, raw_map)
                self.all_objects.append(o)
                return o
            except:
                pass


        
            
    def getObjectByLocationID(self, locationID):
        for o in self.resources:
            if o.origin == locationID and o.isResource:
                return o

            '''
            try:
                location = o.map['core.location']
                if locationID+"://"+locationID == location:
                    return o            
            except:
                pass
            '''
            
    def getObjectByName(self, name):
        for o in self.all_objects:
            if o.name == name:
                return o            

    def __init__(self, username,password,url_base=default_infa_url_base, hawk_url_base=default_infa_hawk_url_base):
        self.all_relationships = []
        self.all_objects = []
        self.businessterms = []
        self.classifications = []
        self.policies = []
        self.resources = []
        self.url_base = url_base
        self.hawk_url_base = hawk_url_base
        self.session_id, self.org_id = self.get_sessionid_and_orgid(username, password)
        self.token = self.get_token(self.session_id, self.org_id)
        ## self.hawk_url_base = 'https://cdgc-api.dm-us.informaticacloud.com'
        print(f"INFO: Fetching Policy Information")
        self.fetchPolicies()
        print(f"INFO: Fetching Resource Information")
        self.fetchResources()
        print(f"INFO: Fetching Classification Information")
        self.fetchClassifications()
        print(f"INFO: Fetching Business Term Information")
        self.fetchBusinessTerms()
        





'''
session = INFASession('reinvent01', 'infa@1234')

for r in session.resources:
    if r.name == 'Snowflake Emea':
        r.fetchObjects()
        print(r.getvalue('core.name')+' '+r.identity+" "+r.origin+' '+str(len(r.objects)))
        for col in r.getObjectsByShortType('Column'):
            print(str(col.getFriendlyId())+" (Class:"+col.getClassificationNames()+") (Terms: "+col.getBusinessTermNames()+")")

## o = session.getObjectByID('89957f4d-ff7f-4053-9612-1eea9e77e904')
print("DEBUG All Objects Count: "+str(len(session.all_objects)))

print("DEBUG: "+str(session.getObjectByLocationID('a3dd21c4-cd07-357c-9050-858d73220fdc').name))
for o in session.all_objects:
    if o.identity == 'a3dd21c4-cd07-357c-9050-858d73220fdc':
        print("Found that object!")

''' 