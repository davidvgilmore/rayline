# C82 native encoder

This helper is Rayline’s narrow process boundary around unmodified upstream
`ggml-org/llama.cpp` tag `b9585`, commit
`d73cd076740db9c111d0e58ddd4486904469e75e`.

It uses libllama’s Qwen3.5 tokenization, recurrent state, token embeddings,
Metal backend, and CUDA backend. Rayline performs the policy’s exact FP32
masked sum/count at this boundary because graph-level
`LLAMA_POOLING_TYPE_MEAN` does not preserve the policy contract for the
multi-sequence recurrent cache shape. Frozen Metal parity passes without an
upstream patch, so Rayline carries no llama.cpp fork.

Configure an audited checkout with:

```sh
cmake -S native/c82-encoder -B build/c82-encoder \
  -DLLAMA_CPP_SOURCE=/path/to/llama.cpp \
  -DGGML_METAL=ON
cmake --build build/c82-encoder --target rayline-c82-encoder -j
```

For CUDA, use `-DGGML_CUDA=ON` and disable Metal. CMake rejects a Git checkout
whose HEAD differs from the pinned revision. Release manifests additionally
pin the helper binary and lossless BF16 GGUF by SHA-256.
