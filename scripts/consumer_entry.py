import subprocess, sys, os
from datascience.config.loader import load_all

def main():
    cfg, _, _ = load_all()
    rc = subprocess.call([sys.executable, "scripts/wait_net.py", cfg["kafka"]["bootstrap"].split(":")[0], cfg["kafka"]["bootstrap"].split(":")[1]])
    if rc != 0:
        print("Kafka not ready"); sys.exit(1)
    
    # Check if we should run enhanced streaming for demo
    if os.getenv("DEMO_MODE") == "true":
        print("🎬 Starting enhanced streaming for demo...")
        # Import and run enhanced streaming with asyncio
        import asyncio
        from scripts.enhanced_streaming import EnhancedStreamProcessor
        
        async def run_enhanced():
            processor = EnhancedStreamProcessor()
            await processor.run_consumer()
        
        asyncio.run(run_enhanced())
    else:
        # Run normal consumer
        from datascience.pipeline.streaming import main as consume_main
        consume_main()

if __name__ == "__main__":
    main()
