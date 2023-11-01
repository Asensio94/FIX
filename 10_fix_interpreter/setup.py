from cx_Freeze import setup, Executable

# Dependencies are automatically detected, but it might need fine-tuning.
build_exe_options = {
    "packages": ["os", "re", "datetime", "streamlit"],
    "include_files": ["20230425-0800_quote.log"],
}

setup(
    name="LiquidityBookApp",
    version="0.1",
    description="Streamlit app for visualizing liquidity book",
    options={"build_exe": build_exe_options},
    executables=[Executable("run_app.py")],
)
