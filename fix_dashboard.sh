# No terminal do VSCode (terminal integrado)
cat > fix_dashboard.sh << 'EOF'
#!/bin/bash
echo "🔧 Corrigindo dashboard PNCP..."
cp dados_pncp.json dados.json
echo "✅ Dashboard pronto!"
EOF

chmod +x fix_dashboard.sh
