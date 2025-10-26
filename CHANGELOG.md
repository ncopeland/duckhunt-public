# Changelog

### v1.0_build94
- **Major Refactor**: Complete removal of JSON backend support
  - Removed all JSON backend else blocks throughout codebase
  - Removed `save_player_data()` calls (129 lines removed)
  - Removed JSON backend logic from `!topduck`, `!duckstats`, duck detector, and clear commands
  - Bot is now SQL-only: cleaner, simpler, more maintainable
- **Enhancement**: Updated default config generation
  - Added SQL backend settings to default config template
  - Added missing `shop_duck_call = 15` configuration
  - Default config now matches current SQL-only setup

### v1.0_build93
- **Enhancement**: !999 command now sends private notice only
  - Hidden feature now truly hidden from channel
  - Players get 999 ammo via private message
  - No channel spam when using the command

### v1.0_build92
- **Major Refactor**: Removed JSON support, SQL-only architecture
  - Removed JSON data storage support
  - Removed migrate_json_to_sql() function
  - Bot now requires SQL backend exclusively
  - Cleaner, more maintainable codebase

### v1.0_build91
- **New Feature**: Added hidden !999 command
  - Gives players 999 ammo instantly
  - Hidden feature, not listed in help commands
  - Requires player authentication to use
- **Security Fix**: Removed hardcoded database credentials
  - Removed hardcoded password from migration function
  - Now requires explicit configuration
  - Prevents credentials from being exposed in code
- **Bug Fix**: Fixed shop item XP refund bugs
  - Fixed items 3, 4, 6, 7, 11 not refunding XP when already active
  - Fixed Safety Lock (item 8) deducting XP instead of refunding
  - All shop items now properly refund XP when purchase fails

### v1.0_build90
- **Bug Fix**: Fixed database initialization hanging
  - Added timeout to prevent infinite hanging during startup
  - Bot now starts with empty player data if database times out
  - Prevents bot from getting stuck at "Starting bot..." message

### v1.0_build89
- **Bug Fix**: Fixed critical CPU usage issue
  - Bot was consuming 100% CPU due to missing sleep delays in main loops
  - Added `await asyncio.sleep(0.01)` when no data is available
  - CPU usage reduced from 87%+ to ~0.5% when idle
  - Fixed both `main_loop()` and `run_network()` methods
- **Enhancement**: Added debug logging for startup process
  - Better visibility into bot initialization phases
  - Helps identify connection and database issues

### v1.0_build88
- **Bug Fix**: Fixed shop XP deduction bug
  - XP no longer deducted when purchasing already active items
  - Prevents players from losing XP on items they can't use
  - Fixed logic order: check availability before deducting XP
- **Enhancement**: Enhanced XP display in shop purchases
  - Added current XP display alongside cost: `[-15 XP] [XP: 207]`
  - XP values now display as integers (removed .0 decimal)
  - Green XP display provides better player feedback
- **Enhancement**: Improved level progression messages
  - Added next level XP requirement to promotions/demotions
  - Format: "126 XP for lvl 6" in level change messages
  - Helps players understand progression requirements
- **Bug Fix**: Fixed critical connection bug
  - Bot no longer hangs in infinite connection retry loops
  - Added break statement after successful connection
  - Bot now properly proceeds to message processing phase
- **Enhancement**: Added enhanced debugging
  - DNS resolution logging with address family details
  - Channel message processing debug logs
  - Better connection state tracking for troubleshooting

### v1.0_build87
- **New Feature**: Added Duck Call shop item (shop 24)
  - Cost: 15 XP
  - Schedules multiple ducks with varying probability:
    - 50% chance: 1 duck
    - 25% chance: 2 ducks
    - 12% chance: 3 ducks
    - 6% chance: 4 ducks
    - 3% chance: 5 ducks
  - Ducks spawn at 1-minute intervals (1st duck after 1min, 2nd after 2min, etc.)
  - Respects max_ducks channel limit
  - Message: "You use the duck call. *QUACK* Duck(s) may arrive any minute now."
- **New Feature**: Channel-specific multilanguage configuration
  - Added `[channel:network:channelname]` sections in config file
  - Settings: `multilang_enabled` (on/off) and `default_language` (language code)
  - Channels can now opt-in/out of multilanguage support
  - Users can only set language preferences if channel has multilang enabled
  - `!ducklang` command respects channel settings
- **Bug Fix**: Fixed soaked status blocking commands incorrectly
  - Soaked players can now befriend ducks (being wet doesn't stop friendship)
  - Soaked players still cannot shoot (wet gun doesn't work)
  - Fixed bug where soaked check happened too late, allowing wild fire shots
- **Bug Fix**: Fixed `!nextduck` not showing duck call scheduled spawns
  - Now checks both normal spawn schedule and duck call schedule
  - Reports the earliest duck spawn time
- **Enhancement**: Updated hiss warning message
  - Changed from "Do not mess with this duck" to yellow "[DO NOT MESS WITH THIS DUCK!]"
  - More visible warning when a duck becomes hostile

### v1.0_build86
- **Bug Fix**: Fixed `!restart` command not working
  - Identified async deadlock: `await asyncio.sleep()` after QUIT messages would hang indefinitely
  - Root cause: Socket connections closing prevented sleep from completing
  - Solution: Call `os._exit(0)` immediately after sending QUIT messages
  - Bot now restarts reliably when owner issues `!restart` command

### v1.0_build85
- **Enhancement**: Added XP ratio feature to stats and leaderboards
  - `!duckstats` now shows XP ratio: (total XP) / (ducks shot + befriended ducks)
  - Smart formatting: xxx (no decimals), xx.x (1 decimal), x.xx (2 decimals)
  - `!topduck xpratio` command to list top players by XP efficiency
  - Helps players understand their efficiency per action
  - Maximum theoretical XP ratio is ~50 under perfect conditions

### v1.0_build84
- **Enhancement**: Fixed timing accuracy in duck detector and !nextduck
  - Duck detector now shows approximate minutes instead of exact seconds
  - !nextduck shows approximate time instead of false precision
  - Messages now say 'approximately Xm' or 'less than 1 minute'
  - Fixes issue where exact timing was misleading due to bot processing delays
  - Users get realistic expectations instead of false precision

### v1.0_build83
- **Enhancement**: Increased duck detector warning time from 60s to 120s
  - Duck detector now warns 2 minutes before spawn instead of 1 minute
  - Gives players more time to prepare and aim
  - Updated message from 'any minute now' to 'soon' for clarity
  - Fixes issue where late processing resulted in 1-2 second warnings

### v1.0_build82
- **Major Fix**: Removed unnecessary channel user tracking for duck detector
  - Duck detector now queries database directly for users with active detectors
  - No longer depends on tracking who's in each channel via JOIN/PART/NAMES
  - Sends notices directly to users based on database records
  - Much simpler and more reliable - always works regardless of user tracking
  - Eliminates all the complexity of normalizing channel names in user lists

### v1.0_build81
- **Critical Fix**: Fixed NAMES (353) response parsing not populating channel user lists
  - Changed condition from `data.startswith("353 ")` to `" 353 " in data`
  - Server messages start with `:servername 353` not `353`, so startswith() never matched
  - Channel user lists now properly populated from NAMES responses
  - Duck detector will now find users in channels and send pre-spawn notices

### v1.0_build80
- **Critical Fix**: Fixed duck detector pre-spawn notices not being sent
  - Bot now normalizes channel names consistently across ALL operations
  - Channel user lists now stored with normalized (lowercase) channel names
  - Fixed JOIN, PART, NAMES (353) message processing to normalize channel names
  - Fixed channel initialization on bot startup to use normalized names
  - Duck detector notices will now be sent to all users with active detectors

### v1.0_build79
- **Critical Fix**: Fixed duck detector not working due to case-sensitive channel names
  - IRC channel names are case-insensitive, but SQL backend was treating them as case-sensitive
  - Bot now normalizes all channel names to lowercase before database operations
  - Fixed issue where #DuckHuntBot and #duckhuntbot were treated as different channels
  - Migration script included to normalize existing database records
  - Duck detector and all per-channel features now work correctly

### v1.0_build78
- **Bug Fix**: Fixed loot message displaying variable name
  - Changed "you find an extra ammo magazine_capacity!" to "you find an extra magazine!"
  - Affects the loot drop when finding magazines in bushes after killing a duck
  - Much clearer and less confusing

### v1.0_build77
- **Enhancement**: Added personalized quit message on restart
  - Bot now shows "{user} requested restart." when quitting
  - QUIT message sent to all connected networks before exiting
  - Makes it clear who triggered the restart
- **Update**: Updated IRC realname field to show v1.0_build77
  - Bot now properly advertises current version when connecting to IRC
  - Version visible in WHOIS and connection messages

### v1.0_build76
- **Enhancement**: Improved promotion/demotion magazine/ammo mechanics
  - When promoted at max magazines (e.g., 2/2), you now get the new max (e.g., 3/3)
  - Same applies to ammo: if at max ammo, you get the new capacity
  - Added feedback messages: "You found a magazine", "You lost a magazine", etc.
  - Demotion properly caps magazines/ammo with loss messages
  - Creates a more rewarding promotion experience

### v1.0_build75
- **Bug Fix**: Fixed demotion magazine/ammo cap issue
  - When demoted, magazines and ammo now immediately cap to new level limits
  - Prevents having 3/2 magazines (over the max) after demotion
  - Caps are calculated including any upgrade levels the player has purchased
  - Ensures consistent weapon capacity across level changes

### v1.0_build74
- **UI Improvement**: Friendlier message for new players using !duckstats
  - New players now see "You haven't shot any ducks yet! Wait for a duck to spawn and try !bang"
  - Prevents confusing "Error retrieving stats" or "No stats found" messages
  - Different message when checking other players: "{player} hasn't shot any ducks yet in {channel}"
  - Applies to both SQL and JSON backends

### v1.0_build73
- **Critical Fix**: Fixed !duckstats crash for new SQL-backend players
  - Fixed "unsupported format string passed to NoneType" error when displaying stats
  - `best_time` and `total_reaction_time` now properly handle NULL values from database
  - New players created directly in SQL backend now have stats displayed correctly
  - Applies to both stats display and reaction time calculations
- **Critical Fix**: Fixed magazine_capacity initialization for SQL-backend players
  - New players now properly start with magazine_capacity=6 and magazines_max=2
  - Fixed incorrect "Ammo: 6/0" display (was backwards due to 0 magazine_capacity)
  - Migration script included to fix existing affected players (49 records corrected)
  - Ammo mechanics now work correctly for all SQL-backend players

### v1.0_build72
- **UI Improvement**: Cleaned up user validation error messages
  - Removed available users list from error messages for cleaner display
  - Error messages now simply state "User 'X' is not in #channel"
  - Applies to !duckstats, !egg, and shop commands (14, 15, 16, 17)

### v1.0_build71
- **New Feature**: Added owner channel management commands
  - !op <channel> <user> - Owner can op channel members
  - !deop <channel> <user> - Owner can deop channel members
  - Commands work on the network where they're received
  - Includes logging and confirmation messages

### v1.0_build70
- **Bug Fix**: Added user existence validation for targeted commands
  - !duckstats now checks if target user exists in channel before showing stats
  - !egg command validates target user exists before attempting to egg them
  - Shop commands (14, 15, 16, 17) now verify target users exist in channel
  - Helpful error messages show available users when target doesn't exist
  - Prevents generic error messages and wasted XP from invalid targets

### v1.0_build69
- **Bug Fix**: Fixed water bucket (shop item 16) duplicate soaking prevention
  - Now checks if target is already soaked before applying effect
  - Refunds XP if target is already soaked (prevents waste)
  - Maintains proper soaked duration stacking behavior

### v1.0_build68
- **New Feature**: Duck resistance mechanics for !bef command
  - Ducks now have 1/20 chance to hiss ferociously on !bef miss
  - Hissed ducks will also thrash any player attempting !bef with -250 XP penalty
  - Thrashing ducks fly away after attacking (prevents further interaction)
  - Added proper level demotion messages for all XP loss scenarios
- **New Feature**: !egg command for veteran players
  - Unlocked after befriending 50 ducks (hidden "easter egg" feature)
  - 24-hour cooldown per player, throws duck egg at target
  - Egged state requires !shop 12 (spare clothes) to remove
  - Egged players can still use !bef but not !bang (prevents gameplay lockout)
  - Added egged status persistence to database schema
- **New Feature**: Enhanced !duckstats display with red status indicators
  - Removed jammed/confiscated from main stats line for cleaner display
  - Added red [Jammed], [Confiscated], and [Egged] indicators at end when active
  - Status indicators only show when conditions are true (no false positives)
  - Improved message length handling to prevent IRC crashes
- **UI Improvement**: Streamlined weapon stats display
  - Cleaner format: shows only ammo and magazines in [Weapon] section
  - Status conditions now displayed as separate red indicators
  - Better visual separation between different stat categories
- **Naming Update**: Renamed "Trigger Lock" to "Safety Lock" throughout
  - Updated shop menu display and purchase messages
  - Changed all references to use "Safety Lock" terminology
  - Maintains consistency with IRC server filtering requirements

### v1.0_build67
- **Bug Fix**: Fixed shop items that affect other players not saving to database
  - Fixed water bucket (item 16): `soaked_until` now saves to database
  - Fixed sabotage (item 17): `jammed` status now saves to database  
  - Fixed mirror (item 14): `mirror_until` now saves to database
  - Fixed sand (item 15): `sand_until` now saves to database
  - Target player effects now persist correctly across bot restarts
  - All shop items that modify other players now work properly

### v1.0_build66
- **Naming Fix**: Completed renaming "Infrared detector" to "Trigger Lock"
  - Fixed shop menu display: now shows "8- Trigger Lock (15 xp)"
  - Fixed purchase message: now says "Trigger Lock enabled for 24h00m"
  - Fixed duplicate purchase message: now says "Trigger Lock already active"
  - Fixed loot drop messages: now says "find a Trigger Lock"
  - All references now consistently use "Trigger Lock" terminology

### v1.0_build65
- **Critical Fix**: Fixed duck kill counter not incrementing in database
  - Root cause: SQL datetime error was preventing database updates from succeeding
  - Fixed `last_duck_time` field to use proper datetime format instead of Unix timestamp
  - Duck kill counter now properly increments: 8 → 9 → 10 ducks, etc.
  - Database persistence now works correctly for all stat changes
  - Removed debug logging after successful fix

### v1.0_build64
- **Bug Fix**: Fixed `!lastduck` command showing incorrect data
  - `!lastduck` was using in-memory `channel_last_duck_time` dictionary instead of database
  - Now properly reads `last_duck_time` and `ducks_shot` from database
  - Duck kill count and timing now consistent between `!bang` and `!lastduck` commands
  - Fixes issue where `!lastduck` showed "No ducks killed" despite kills being recorded

### v1.0_build63
- **Database Schema Fix**: Added missing shop item columns
  - Added `clover_until`, `clover_bonus` for four-leaf clover (item #10)
  - Added `brush_until` for gun brush (item #13)
  - Added `sight_next_shot` for sight attachment (item #7)
  - Note: Trigger Lock (item #8) uses `trigger_lock_until`, `trigger_lock_uses` (already existed)
  - All 23 shop items now have complete database support
  - Shop item purchases now persist correctly in SQL backend
  - Migration script provided in `migrations/add_shop_items_columns.sql`
  - Schema audit document created: `SCHEMA_AUDIT.md`

### v1.0_build62
- **Critical Fix**: Fixed ammo persistence when hitting golden ducks
  - Golden duck reveal was returning without saving ammo consumption
  - All shots (hit, miss, golden duck) now properly save ammo decrements

### v1.0_build61
- **Critical Fix**: Fixed magazine/ammo persistence for SQL backend
  - `get_channel_stats()` now loads fresh from database for SQL backend
  - Removed hybrid in-memory/SQL cache that caused stale data
  - Magazine decrements from `!reload` now persist correctly
  - All stat modifications now save and load properly

### v1.0_build60
- **Bug Fix**: Fixed Undernet MOTD detection
  - Added handling for IRC 422 (MOTD File is missing) response
  - Undernet now connects immediately instead of waiting 180 seconds for timeout
  - Changed Undernet server to chicago.il.us.undernet.org for better latency

### v1.0_build59
- **Bug Fix**: Fixed misses not saving ammo consumption
  - Moved save logic outside of ricochet victim check
  - All misses now properly save ammo decrements and XP penalties
  - Stats persist correctly after every shot (hit or miss)

### v1.0_build58
- **Critical Bug Fix #3**: Fixed stats not saving to SQL database at all
  - Corrected `_filter_computed_stats()` to only filter penalty/reliability fields
  - `magazine_capacity` and `magazines_max` are persistent upgrade fields and now save correctly
  - All stat changes (ammo, magazines, XP, etc.) now properly persist to database
  - Database timestamps now update correctly after each action

### v1.0_build57
- **Bug Fix Attempt**: Created filtering system for computed stats (had incorrect field list)

### v1.0_build56
- **Critical Bug Fix**: Fixed magazine/ammo stats not saving to SQL database
  - All `!bang`, `!reload`, `!bef`, `!shop`, and admin commands now properly persist state changes to database
  - Magazine count in `!duckstats` now correctly reflects actual remaining magazines
  - Ammo consumption and reload actions are now properly saved
  - Replaced all `save_player_data()` calls with explicit SQL `update_channel_stats()` when using SQL backend

### v1.0_build55
- **Multilanguage Foundation**: Added complete multilanguage support system
  - Created LanguageManager class with IRC color preservation
  - Added 25 language files (English complete, 24 stubs ready for translation)
  - Implemented `!ducklang` command for users to change language preference
  - Color markers (`{{red:text}}`, `{{bold:text}}`) preserve IRC formatting in translations
  - User language preferences saved to `language_prefs.json`
  - Supported languages: English, Spanish, French, German, Russian, Japanese, Mandarin Chinese, Hindi, Arabic, Portuguese, Bengali, Urdu, Indonesian, Nigerian Pidgin, Marathi, Egyptian Arabic, Telugu, Turkish, Tamil, Cantonese, Vietnamese, Wu Chinese, Tagalog, Korean, and Farsi
- **Duck Detector Improvements**: Fixed duplicate purchase bug and added immediate notice when purchased with spawn imminent
- **Documentation**: Added `MULTILANG_ROADMAP.md` with implementation plan for full bot message refactoring
- System ready for incremental translation of bot messages (Phase 1 pending)

### v1.0_build54
- **Rate Limiting**: Added 1 message per second rate limiting per network to prevent flood issues
- **Code Cleanup**: Removed temporary migration and debug scripts from repository
- **Bug Fixes**: Fixed various SQL backend issues with Decimal/float conversions
- Removed duckhunt.data.backup from repo

### v1.0_build53
- Implement lossless backup/restore system for clear command
- Fix clear command for SQL backend and improve network connectivity
- Add MariaDB/MySQL SQL backend support
- Add missing handler methods for commands

### v1.0_build52
- Fix SSL connections for networks requiring secure connections
- Add IPv6 support for servers with IPv6-only interfaces
- Fix magazine capacity upgrade logic (no longer magically adds ammo)
- Replace all "clip" terminology with proper "magazine" terminology
- Fix explosive ammo to decrement on all shots, not just golden ducks
- Rename "infrared detector" to "trigger lock" throughout codebase
- Fix trigger lock purchase confirmation and message visibility
- Fix clear command counting bug and recover lost player data
- Fix data migration to handle legacy field names
- Fix duplicate channel name conflicts across networks with network-prefixed keys
- Fix duck counting logic - regular and golden ducks each count as one
- Add `!topduck duck` command to sort by ducks killed instead of XP
- Enhance `!duckstats` with network/channel display, XP breakdown, and items section

### v1.0_build51
- Fix golden duck survival message colorization
- Fix duck kill message spacing (remove extra space after channel name)

### v1.0_build50
- Fix promotion/demotion message spacing and colorization
- Fix "you missed" message spacing and red colorization
- Fix "empty magazine" message - colorize *CLICK* red
- Fix "no magazines left" message content
- Fix "you reload" message - colorize *CLACK CLACK* red
- Fix "gun doesn't need reload" message - remove grey colorization
- Ensure shop XP penalties are consistently red
- Fix sunglasses purchase logic to prevent duplicate purchases
- Add life remaining display to golden duck hit messages
- Change `!bang` to show remaining duck health instead of damage dealt
- Remove green colorization from shop purchase messages (AP ammo, grease, sight)
- Improve trigger locked message formatting and colorization

### v1.0_build49-48
- Add `!part` command for bot owner to leave channels with proper cleanup
- Fix befriend command message formatting and crashes
- Remove white colorization from various messages
- Add debug logging to duck detector notification system

### Earlier Builds (v1.0_build47 and below)
- Fix duck despawn functionality to properly clean up after 700 seconds
- Fix spawn scheduling to only trigger on appropriate events
- Add MOTD handling for proper bot registration
- Multi-network support with network-specific configurations
- Full shop system with 22+ items
- Karma and XP tracking system
- Level progression with bonuses
- Golden duck mechanics with multi-hit system
- Magazine and ammunition management
- Various items: detector, trigger lock, silencer, insurance, etc.

