import React, { useState, useEffect } from 'react';

const API_BASE_URL = 'https://hycqu1nzxl.execute-api.ap-southeast-2.amazonaws.com'; // 例如: https://xxxx.execute-api.region.amazonaws.com

export default function OrderStatusTracker({ orderId }) {
  const [status, setStatus] = useState('PENDING');
  const [orderDetails, setOrderDetails] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!orderId) return;

    // 定義輪詢函式
    const fetchOrderStatus = async () => {
      try {
        const response = await fetch(`${API_BASE_URL}/orders/${orderId}`);
        
        if (!response.ok) {
          if (response.status === 404) {
            // 如果訂單剛送出，Worker 可能還沒寫入 PENDING，可以繼續等
            return;
          }
          throw new Error('Unable to obtain order status');
        }

        const data = await response.json();
        setOrderDetails(data);
        const currentStatus = data.status;
        setStatus(currentStatus);

        // 如果狀態已經是終態 (COMPLETED 或 CANCELLED)，停止輪詢
        if (currentStatus === 'COMPLETED' || currentStatus === 'CANCELLED') {
          clearInterval(pollInterval);
          setLoading(false);
        }
      } catch (err) {
        console.error('An error occurred while querying the order:', err);
        setError(err.message);
      }
    };

    // 立即執行一次，然後每 3 秒輪詢一次
    fetchOrderStatus();
    const pollInterval = setInterval(fetchOrderStatus, 3000);

    // 當元件卸載或 orderId 改變時，清除定時器
    return () => clearInterval(pollInterval);
  }, [orderId]);

  // 根據不同狀態顯示對應的視覺風格
  const getStatusBadge = (st) => {
    switch (st) {
      case 'PENDING':
        return <span style={{ color: '#f59e0b' }}>⏳ Order processing (Saga in progress...)</span>;
      case 'COMPLETED':
        return <span style={{ color: '#10b981' }}>✅ Transaction successful! Inventory and payment completed.</span>;
      case 'CANCELLED':
        return <span style={{ color: '#ef4444' }}>❌ The transaction failed, and a compensation rollback has been executed.</span>;
      default:
        return <span>🔍 Looking for orders...</span>;
    }
  };

  return (
    <div style={{ padding: '20px', border: '1px solid #ccc', borderRadius: '8px', maxWidth: '400px' }}>
      <h3>Order Tracker</h3>
      <p><strong>Order Number:</strong> {orderId}</p>
      
      <div style={{ margin: '15px 0', fontSize: '16px' }}>
        <strong>Current status:</strong> {getStatusBadge(status)}
      </div>

      {error && <p style={{ color: 'red' }}>Error: {error}</p>}

      {orderDetails && (
        <div style={{ background: '#f9f9f9', padding: '10px', borderRadius: '4px', fontSize: '14px' }}>
          <p><strong>Purchase goods:</strong> {JSON.stringify(orderDetails.items)}</p>
          <p><strong>Last Update:</strong> {new Date(orderDetails.updated_at * 1000).toLocaleTimeString()}</p>
        </div>
      )}
    </div>
  );
}