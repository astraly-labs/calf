# calf

A smol implementation of [narwhal](https://arxiv.org/pdf/2105.11827) - a mempool narwhal DAG-based consensus mechanism.

<p align="center">
  <img src="./assets/calf.png" alt="calf logo" width="300"/>
</p>

## 🚀 Getting Started

### Prerequisites

- 🐍 Python 3.9+
- 📦 UV package manager
- 🍺 Homebrew (for macOS users)

### System Dependencies

**For macOS:**
```bash
brew install gmp
```

### 🔧 Environment Setup

Set up your development environment with these steps:

```bash
# Create and activate venv
uv venv
source .venv/bin/activate

# Install dependencies
uv pip install -r requirements.txt
```

## 🏃‍♂️ Running the Project

### Basic Usage

```bash
python test_launcher.py --validators <number_of_validators> [optional arguments]
```

#### Arguments

**Required:**
- `--validators`: Number of validators to run

**Optional:**
- `--workers`: Number of workers per validator (default: 1)
- `--test-id`: Test name (default: "test")
- `--calf`: Path to the executable (default: "target/release/calf")

#### File Requirements

Before running, ensure you have:
- ✅ The calf executable at `target/release/calf` (or specify a different path with `--calf`)
- ✅ A `committee.json` file in your working directory (or specify a different path with `--committee-path`)

## 📚 Learning Resources

Learn more about Narwhal and DAG-based consensus:

- [Sui's Narwhal Implementation](https://github.com/MystenLabs/sui/tree/main/narwhal)
- [Narwhal and Tusk Research Paper](https://arxiv.org/pdf/2105.11827)
- [Delphi Digital's Narwhal Primer](https://members.delphidigital.io/feed/a-primer-on-narwhal)

### Video Resources
- [Narwhal & Tusk: A DAG-based Mempool and Efficient BFT Consensus](https://www.youtube.com/watch?v=xKDDuPrYUag)
- [Deep Dive into Narwhal & Tusk](https://www.youtube.com/watch?v=K5ph4-7vvHk)
- [Narwhal and Tusk: A DAG-based Mempool and BFT Consensus](https://www.youtube.com/watch?v=NGOXVSFzYdI&t=2018s)
- [Narwhal/Bullshark: DAG-based Mempool and Efficient BFT Consensus](https://www.youtube.com/watch?v=v7h2rXNtrV0)
