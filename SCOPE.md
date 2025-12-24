# SCOPE

## WBS
- A direct Rust conversion of the project /projects/clickhouse-loader into this project
- Read All the MD files and review the project structure for tests intent and modularity 
- Discuss how we implement a hs-rustlib private artifactory (equivalent of hs-golib and /projects/hs-lib)
- discuss using ARROW vs other structures for our use case - for decision point clickhouse-arrow vs Klickhouse
- Decide on the clickhouse library to use CLICKHOUSE_RUST_NATIVE_PROTOCOL_DECISION.md
- zero-copy paths are important! ideally kafka->struct ready to ch load->processing on that struct->clickhous
- Create a WBS.md for the work to perform

## ONCE DESIGN AND WBS IS IN PLACE

Discuss how I can run this in sandbox mode to reduce prompts for the first pass  
