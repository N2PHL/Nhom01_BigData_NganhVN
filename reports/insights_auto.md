# Insight tự động từ bảng Gold (sinh bởi src/insights.py)

## Q1. Ngành nào có lợi suất điều chỉnh rủi ro tốt nhất so với VN-Index, và thứ hạng có ổn định qua từng năm?

- VN-Index cả giai đoạn: CAGR 9,9%, biến động 18,8%/năm, Sharpe 0,45, sụt giảm tối đa -45,3%.
- Dẫn đầu theo Sharpe là **Bán lẻ** (CAGR 20,7%, Sharpe 0,77, MaxDD -49,7%); thấp nhất là **Tài nguyên cơ bản** (CAGR 6,7%, Sharpe 0,27).
- 9/11 ngành (rổ bình quân đều) có CAGR cao hơn VN-Index: Bán lẻ, Hàng & Dịch vụ công nghiệp, Dịch vụ tài chính, Ngân hàng, Hóa chất, Điện, nước & xăng dầu khí đốt, Bất động sản, Thực phẩm & Đồ uống, Dầu khí.
- Tính ổn định (9 năm đủ dữ liệu): ngành lọt top 3 nhiều năm nhất là **Bất động sản** (5/9 năm); thứ hạng biến động ít nhất là **Hàng & Dịch vụ công nghiệp**, nhiều nhất là **Điện, nước & xăng dầu khí đốt** → hiệu quả ngành có tính chu kỳ, không nên ngoại suy 1–2 năm gần nhất.

## Q2. Ngành nào phòng thủ, ngành nào tấn công (beta, upside/downside capture, hiệu quả khi VN-Index dưới MA200)?

- Beta thấp nhất: **Điện, nước & xăng dầu khí đốt** (0,67); cao nhất: **Dịch vụ tài chính** (1,45).
- Ngành có đặc tính phòng thủ (beta < 1 và downside capture < 1): Bán lẻ (down 0,84), Hàng & Dịch vụ công nghiệp (down 0,77), Hóa chất (down 0,80), Điện, nước & xăng dầu khí đốt (down 0,59), Thực phẩm & Đồ uống (down 0,74), Xây dựng & Vật liệu (down 0,90).
- Khi VN-Index dưới MA200, ngành giữ giá tốt nhất so với thị trường: **Điện, nước & xăng dầu khí đốt** (vượt 27,9%/năm); kém nhất: **Bất động sản** (-8,6%/năm).
- Khi VN-Index trên MA200, ngành vượt trội nhất: **Dịch vụ tài chính** (vượt 27,1%/năm).

## Q3. Dòng tiền (giá trị giao dịch) dịch chuyển giữa các ngành ra sao; ngành nào đang hút hoặc thoát tiền?

- Giá trị giao dịch bình quân/phiên của 81 mã thay đổi từ 546 tỷ đồng (2017-Q1) lên 8028 tỷ đồng (2026-Q4).
- Quý gần nhất (2026-Q4) ngành chiếm tỷ trọng thanh khoản lớn nhất: **Ngân hàng** (23,3%).
- So 4 quý gần nhất với 4 quý trước đó: hút tiền mạnh nhất **Dầu khí** (+4,8 điểm % tỷ trọng), thoát tiền mạnh nhất **Dịch vụ tài chính** (-4,8 điểm %).

## Q4. Các ngành tương quan với nhau đến đâu; cặp ngành nào giúp đa dạng hóa danh mục tốt nhất?

- Tương quan lợi suất ngày trung bình giữa các cặp ngành: 0,62 → các ngành cùng chịu nhân tố thị trường chung, đa dạng hóa chỉ theo ngành có tác dụng giới hạn.
- Cặp ít tương quan nhất: **Dầu khí – Bất động sản** (0,48); Ngân hàng – Hóa chất (0,48).
- Cặp tương quan cao nhất: **Hàng & Dịch vụ công nghiệp – Bán lẻ** (0,71) → nắm cả hai gần như không giảm rủi ro.

## Q5. Trong từng ngành, mã nào dẫn dắt và mức độ phân hóa giữa các mã cùng ngành lớn đến đâu?

- Mã dẫn dắt theo Sharpe ở mỗi ngành: Bất động sản: **VIC** (0,91); Thực phẩm & Đồ uống: **ANV** (0,81); Bán lẻ: **DGW** (0,77); Ngân hàng: **MBB** (0,76); Dịch vụ tài chính: **SHS** (0,74); Hàng & Dịch vụ công nghiệp: **HAH** (0,68); Điện, nước & xăng dầu khí đốt: **TDM** (0,59); Hóa chất: **DCM** (0,57); Tài nguyên cơ bản: **DHC** (0,57); Dầu khí: **PVS** (0,41); Xây dựng & Vật liệu: **BMP** (0,40).
- Phân hóa nội ngành (chênh lệch CAGR mã tốt nhất – kém nhất) lớn nhất ở **Bất động sản** (40,5%), nhỏ nhất ở **Dầu khí** (12,3%) → ngành phân hóa mạnh cần chọn mã; ngành đồng pha có thể tiếp cận theo rổ/ETF ngành.

## Khuyến nghị dựa trên dữ liệu

- Danh mục lõi dài hạn có thể ưu tiên các ngành vừa có Sharpe trên trung vị vừa có downside capture dưới trung vị: Bán lẻ, Hàng & Dịch vụ công nghiệp, Hóa chất, Điện, nước & xăng dầu khí đốt.
- Chỉ nên tăng tỷ trọng khi VN-Index trên MA200, giảm khi VN-Index dưới MA200 (vượt thị trường lúc tăng, kém thị trường lúc giảm): Bán lẻ, Tài nguyên cơ bản, Bất động sản.
- Nơi trú ẩn khi VN-Index dưới MA200 (vượt thị trường lúc giảm, kém lúc tăng): Điện, nước & xăng dầu khí đốt, Xây dựng & Vật liệu, Thực phẩm & Đồ uống.
- Vượt VN-Index hơn 10%/năm ở cả hai chế độ: Ngân hàng.
- Do tương quan giữa các ngành cao, cần kết hợp thêm quản trị tỷ trọng tiền mặt theo chế độ thị trường thay vì chỉ đa dạng hóa theo ngành.
- Đây là phân tích dữ liệu lịch sử cho mục đích học thuật, chưa tính chi phí giao dịch/thuế và không phải khuyến nghị mua bán.
