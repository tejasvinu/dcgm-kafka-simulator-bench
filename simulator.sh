#!/bin/bash

# Set strict error handling
set -euo pipefail

# Install required Python packages
pip install lz4

# Configuration
METRICS_SERVER="dcgm_sim_test"  # Assuming this is your FastAPI app
CONSUMER_SCRIPT="kafka_consume.py"
PRODUCER_SCRIPT="kafka_prod.py"
BASE_LOG_DIR="benchmark_logs"
STARTUP_WAIT=60          # Time to wait for server startup
WARMUP_PERIOD=300       # 5 minute warmup
TEST_DURATION=1800      # 30 minute test duration (excluding warmup)
SHUTDOWN_WAIT=60        # Time for graceful shutdown
METRICS_INTERVAL=30     # Collect metrics every 30 seconds
SERVER_PORT=50000  # Single port for the server
NUM_SERVER_WORKERS=4 # Number of Uvicorn workers (adjust as needed)

# Test configurations
# Format: "num_nodes:num_processes"
declare -a configs=(
"8:2"
"16:4"
"32:4"
"64:8"
"128:8"
"256:16"
"512:16"
"1024:32"
"2048:32"
"4096:64"
"8192:64"
)

# Create log directory with timestamp
timestamp=$(date +%Y%m%d_%H%M%S)
LOG_DIR="${BASE_LOG_DIR}/${timestamp}"
mkdir -p "$LOG_DIR"

# Log file for the entire benchmark run
MAIN_LOG="${LOG_DIR}/benchmark_main.log"

# Function to log messages
log() {
  local message="$1"
  local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
  echo "[${timestamp}] ${message}" | tee -a "$MAIN_LOG"
}

# Add before starting benchmarks - after log() function definition
setup_system_limits() {
    local current_limit=$(ulimit -n)
    local target_limit=65536
    local updated=false

    log "Current file descriptor limit: $current_limit"

    # First try setting user limits without sudo
    if ulimit -n "$target_limit" 2>/dev/null; then
        log "Successfully set file descriptor limit to $target_limit"
        updated=true
    fi

    # If we couldn't set the limit and have sudo, try with sudo
    if [ "$updated" = false ] && command -v sudo >/dev/null 2>&1; then
        log "Attempting to set system limits with sudo..."
        
        # Try to update system limits
        if sudo sysctl -w fs.file-max="$target_limit" >/dev/null 2>&1; then
            if sudo sysctl -w fs.nr_open="$target_limit" >/dev/null 2>&1; then
                log "Successfully updated system-wide limits"
            fi
        fi

        # Try to update current session limits with sudo
        if sudo bash -c "ulimit -n $target_limit" >/dev/null 2>&1; then
            log "Successfully set session file descriptor limit"
            updated=true
        fi
    fi

    if [ "$updated" = false ]; then
        log "WARNING: Could not increase file descriptor limits"
        log "Current limit of $current_limit will be used"
        
        # Calculate max nodes based on current limit
        local max_nodes=$((current_limit/4))  # Using 4 file descriptors per node as estimate
        log "Based on current limits, recommended max nodes: $max_nodes"
        
        # Filter configs array to only include supported node counts
        local old_configs=("${configs[@]}")
        configs=()
        for config in "${old_configs[@]}"; do
            local num_nodes=${config%%:*}
            if [ "$num_nodes" -le "$max_nodes" ]; then
                configs+=("$config")
            else
                log "Skipping configuration with $num_nodes nodes (exceeds limit)"
            fi
        done
        
        if [ ${#configs[@]} -eq 0 ]; then
            log "ERROR: No valid configurations remain within system limits"
            exit 1
        fi
        
        log "Proceeding with ${#configs[@]} valid configurations"
    fi
}

# Add this function to validate configuration before starting servers
validate_configuration() {
    local config=$1
    local num_nodes=${config%%:*}
    local current_limit=$(ulimit -n)
    local estimated_fds=$((num_nodes * 4))  # Estimate 4 FDs per node
    
    if [ "$estimated_fds" -gt "$((current_limit * 9 / 10))" ]; then
        log "WARNING: Configuration with $num_nodes nodes may exceed 90% of file descriptor limit ($current_limit)"
        log "This could lead to stability issues"
        read -p "Do you want to continue with this configuration? (y/N) " -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            return 1
        fi
    fi
    return 0
}

# Function to check if a process is running
is_process_running() {
  local pid=$1
  if ps -p "$pid" > /dev/null; then
    return 0
  else
    return 1
  fi
}

# Function to kill process and its children
kill_process_tree() {
  local pid=$1
  if is_process_running "$pid"; then
    pkill -P "$pid" 2>/dev/null || true
    kill -15 "$pid" 2>/dev/null || kill -9 "$pid" 2>/dev/null || true
  fi
}

# Add resource monitoring
monitor_resources() {
    while true; do
        timestamp=$(date +%s)
        cpu_usage=$(top -bn1 | grep "Cpu(s)" | sed "s/.*, *\([0-9.]*\)%* id.*/\1/" | awk '{print 100 - $1}')
        mem_usage=$(free | grep Mem | awk '{print $3/$2 * 100.0}')
        disk_usage=$(df -h / | tail -1 | awk '{print $5}' | sed 's/%//')
        
        echo "$timestamp,$cpu_usage,$mem_usage,$disk_usage" >> "${LOG_DIR}/resource_usage.csv"
        sleep 10
    done
}

# Function to cleanup processes
cleanup() {
  log "Cleaning up processes..."
  
  # Force kill processes in correct order
  for pid in "${producer_pid}" "${consumer_pid}" "${server_pid}"; do
      if [[ -n "${pid}" ]]; then
          pkill -KILL -P "${pid}" 2>/dev/null || true  # Kill children first
          kill -9 "${pid}" 2>/dev/null || true         # Force kill parent
      fi
  done
  
  # Stop resource monitoring
  if [[ -n "${monitor_pid}" ]]; then
      kill -9 "${monitor_pid}" 2>/dev/null || true
  fi
  
  # Archive logs
  if [[ -d "${LOG_DIR}" ]]; then
      tar -czf "${LOG_DIR}.tar.gz" "${LOG_DIR}"
      log "Logs archived to ${LOG_DIR}.tar.gz"
  fi
}

# Set trap for cleanup
trap cleanup EXIT INT TERM

# Function to run the benchmark for a given configuration
run_benchmark() {
  local config=$1
  
  # Validate configuration before proceeding
  if ! validate_configuration "$config"; then
      log "Skipping configuration $config due to resource constraints"
      return 0
  fi

  local num_nodes=${config%%:*}
  local num_processes=${config##*:}

  log "Starting benchmark with warmup period..."
  log "Warmup: $WARMUP_PERIOD seconds"
  log "Test duration: $TEST_DURATION seconds"

  # Start resource monitoring with timestamp
  local test_start_time=$(date +%s)

  local benchmark_log="${LOG_DIR}/benchmark_${num_nodes}_nodes.log"
  local consumer_log="${LOG_DIR}/consumer_${num_nodes}_nodes.log"
  local producer_log="${LOG_DIR}/producer_${num_nodes}_nodes.log"
  local server_log="${LOG_DIR}/server_${num_nodes}_nodes.log"

  log "Starting benchmark for $num_nodes nodes with $num_processes processes..."

  # Start the metrics server with num_nodes number of ports (one per node)
  log "Starting metrics server on ports starting from $SERVER_PORT with $num_nodes nodes..."
  python3 "$METRICS_SERVER.py" \
    --num_ports "$num_nodes" \
    --start_port "$SERVER_PORT" \
    --total_nodes "$num_nodes" > "$server_log" 2>&1 &
  server_pid=$!

  # Wait for server startup and verify
  log "Waiting $STARTUP_WAIT seconds for server startup..."
  sleep "$STARTUP_WAIT"

  # Modified server verification
  # Check if at least one server process is running
  if ! ps -p "$server_pid" > /dev/null; then
    log "ERROR: Main server process failed to start. Check server logs at $server_log"
    return 1
  fi

  # Check if we can connect to at least one server port
  if ! curl -s "http://localhost:$SERVER_PORT/health" > /dev/null; then
    log "ERROR: Server health check failed. Check server logs at $server_log"
    kill_process_tree "$server_pid"
    return 1
  fi

  # Start the consumer
  log "Starting consumer..."
  python3 "$CONSUMER_SCRIPT" > "$consumer_log" 2>&1 &
  consumer_pid=$!

  # Verify consumer is running
  sleep 5
  if ! is_process_running "$consumer_pid"; then
    log "ERROR: Consumer failed to start. Check consumer logs at $consumer_log"
    return 1
  fi

  # Start the producer
  log "Starting producer with $num_nodes nodes and $num_processes processes..."
  python3 "$PRODUCER_SCRIPT" --num_nodes "$num_nodes" --num_processes "$num_processes" > "$producer_log" 2>&1 &
  producer_pid=$!

  # Wait for warmup + test duration
  log "Running warmup period for $WARMUP_PERIOD seconds..."
  sleep "$WARMUP_PERIOD"
  log "Warmup complete, starting main test for $TEST_DURATION seconds..."
  sleep "$TEST_DURATION"

  # Graceful shutdown
  log "Initiating graceful shutdown..."

  # Stop producer first
  kill_process_tree "$producer_pid"
  wait "$producer_pid" 2>/dev/null || true

  # Stop consumer
  kill_process_tree "$consumer_pid"
  wait "$consumer_pid" 2>/dev/null || true

  # Stop server last
  kill_process_tree "$server_pid" # This will kill the waitress-serve master process
  wait "$server_pid" 2>/dev/null || true

  # Wait for complete shutdown
  sleep "$SHUTDOWN_WAIT"

  log "Benchmark for $num_nodes nodes completed"
  log "Logs saved to:"
  log "  Server:   $server_log"
  log "  Consumer: $consumer_log"
  log "  Producer: $producer_log"
  echo ""
}

# Start resource monitoring in background
monitor_resources &
monitor_pid=$!

# Main benchmark execution
setup_system_limits
log "Starting benchmark suite"
log "Log directory: $LOG_DIR"

for config in "${configs[@]}"; do
  log "=========================================="
  if run_benchmark "$config"; then
    log "Successfully completed benchmark for ${config%%:} nodes with ${config#:} processes"
  else
    log "Failed benchmark for ${config%%:} nodes with ${config#:} processes"
  fi
  log "=========================================="
  echo ""
  # Add small delay between tests
  sleep 5
done

# Run analysis and visualization
log "Running analysis and visualization..."
python3 analyze_throughput.py > "${LOG_DIR}/analysis_results.log" 2>&1

# Archive analysis results and visualizations
log "Archiving analysis results and visualizations..."
tar -czf "${LOG_DIR}_analysis.tar.gz" "${LOG_DIR}/throughput_analysis.png" "${LOG_DIR}/analysis_summary.txt" "${LOG_DIR}/analysis_results.log"

log "All benchmarks completed"
log "Results and logs available in: $LOG_DIR"